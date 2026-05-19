package service

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/new-api-tools/backend/internal/cache"
	"github.com/new-api-tools/backend/internal/logger"
)

const (
	aiBanConfigKey              = "ai_ban:config"
	aiBanAuditLogsKey           = "ai_ban:audit_logs"
	aiBanAPIHealthKey           = "ai_ban:api_health"
	aiBanLastScanKey            = "ai_ban:last_scan_at"
	aiBanAuditLogLimit          = 1000
	aiBanAutoBanRiskScore       = 8.0
	aiBanAutoBanConfidence      = 0.75
	aiBanFailureSuspendCount    = 3
	aiBanFailureSuspendDuration = 30 * time.Minute
)

const defaultAIBanPrompt = `你是 New API 风控审查助手。请根据用户近期调用画像判断是否存在滥用、撞库、批量薅额度、异常代理池、恶意空回复消耗或高失败率探测行为。

只允许输出一个 JSON 对象，不要输出 Markdown 或解释文字。字段必须为：
{
  "should_ban": boolean,
  "risk_score": number,
  "confidence": number,
  "action": "normal" | "monitor" | "warn" | "ban",
  "reason": "简短中文理由"
}

评分规则：
- 1-3：正常或证据不足。
- 4-6：需要观察。
- 7：明显可疑，建议告警。
- 8-10：证据充分，才建议封禁。

请保持保守：只有在证据充分且置信度较高时才输出 ban。confidence 使用 0 到 1 的小数。`

var aiBanScanMu sync.Mutex

type aiBanAPIHealthState struct {
	ConsecutiveFailures int64  `json:"consecutive_failures"`
	LastError           string `json:"last_error"`
	LastFailureAt       int64  `json:"last_failure_at"`
	SuspendedUntil      int64  `json:"suspended_until"`
}

type aiBanAssessment struct {
	ShouldBan        bool    `json:"should_ban"`
	ModelShouldBan   bool    `json:"model_should_ban,omitempty"`
	RiskScore        float64 `json:"risk_score"`
	Confidence       float64 `json:"confidence"`
	Action           string  `json:"action"`
	Reason           string  `json:"reason"`
	PromptTokens     int     `json:"prompt_tokens,omitempty"`
	CompletionTokens int     `json:"completion_tokens,omitempty"`
	APIDurationMS    int64   `json:"api_duration_ms,omitempty"`
	Model            string  `json:"model,omitempty"`
	RawResponse      string  `json:"raw_response,omitempty"`
}

type aiBanModelError struct {
	Message          string
	RawResponse      string
	PromptTokens     int
	CompletionTokens int
	Model            string
	StatusCode       int
}

func (e *aiBanModelError) Error() string {
	return e.Message
}

type aiBanProtection struct {
	Protected bool
	Reason    string
	User      map[string]interface{}
}

type aiBanAssessOptions struct {
	config       map[string]interface{}
	source       string
	writeHealth  bool
	manualResult bool
}

func copyDefaultAIBanConfig() map[string]interface{} {
	config := make(map[string]interface{}, len(defaultAIBanConfig))
	for k, v := range defaultAIBanConfig {
		switch list := v.(type) {
		case []string:
			copied := make([]string, len(list))
			copy(copied, list)
			config[k] = copied
		default:
			config[k] = v
		}
	}
	return config
}

func (s *AIAutoBanService) getRawAIBanConfig() map[string]interface{} {
	cm := cache.Get()
	var config map[string]interface{}
	found, _ := cm.GetJSON(aiBanConfigKey, &config)
	if !found || config == nil {
		config = copyDefaultAIBanConfig()
	}
	for k, v := range defaultAIBanConfig {
		if _, ok := config[k]; !ok {
			config[k] = v
		}
	}
	return config
}

func configString(config map[string]interface{}, key string) string {
	if config == nil {
		return ""
	}
	return strings.TrimSpace(toString(config[key]))
}

func configBool(config map[string]interface{}, key string, fallback bool) bool {
	if config == nil {
		return fallback
	}
	switch v := config[key].(type) {
	case bool:
		return v
	case string:
		if v == "" {
			return fallback
		}
		return strings.EqualFold(v, "true") || v == "1"
	default:
		if _, ok := config[key]; ok {
			return toInt64(config[key]) != 0
		}
		return fallback
	}
}

func configInt(config map[string]interface{}, key string, fallback int64) int64 {
	if config == nil {
		return fallback
	}
	if _, ok := config[key]; !ok {
		return fallback
	}
	return toInt64(config[key])
}

func configStringSlice(config map[string]interface{}, key string) []string {
	if config == nil {
		return []string{}
	}
	return toStringSlice(config[key])
}

func toStringSlice(v interface{}) []string {
	switch list := v.(type) {
	case []string:
		out := make([]string, 0, len(list))
		for _, item := range list {
			if trimmed := strings.TrimSpace(item); trimmed != "" {
				out = append(out, trimmed)
			}
		}
		return out
	case []interface{}:
		out := make([]string, 0, len(list))
		for _, item := range list {
			if trimmed := strings.TrimSpace(toString(item)); trimmed != "" {
				out = append(out, trimmed)
			}
		}
		return out
	case string:
		if strings.TrimSpace(list) == "" {
			return []string{}
		}
		parts := strings.FieldsFunc(list, func(r rune) bool {
			return r == ',' || r == '\n' || r == '\r' || r == ';'
		})
		out := make([]string, 0, len(parts))
		for _, item := range parts {
			if trimmed := strings.TrimSpace(item); trimmed != "" {
				out = append(out, trimmed)
			}
		}
		return out
	default:
		return []string{}
	}
}

func (s *AIAutoBanService) validateAIBanRuntimeConfig(config map[string]interface{}) error {
	if configString(config, "base_url") == "" {
		return errors.New("未配置 API Base URL")
	}
	if configString(config, "api_key") == "" {
		return errors.New("未配置 API Key")
	}
	if configString(config, "model") == "" {
		return errors.New("未选择 AI 模型")
	}
	return nil
}

func (s *AIAutoBanService) getWhitelistIDs() []int64 {
	var whitelist []interface{}
	cache.Get().GetJSON("ai_ban:whitelist", &whitelist)
	ids := make([]int64, 0, len(whitelist))
	for _, item := range whitelist {
		id := toInt64(item)
		if id > 0 {
			ids = append(ids, id)
		}
	}

	if len(ids) == 0 {
		var typed []int64
		cache.Get().GetJSON("ai_ban:whitelist", &typed)
		ids = typed
	}
	return ids
}

func (s *AIAutoBanService) isWhitelistedUser(userID int64) bool {
	for _, id := range s.getWhitelistIDs() {
		if id == userID {
			return true
		}
	}
	return false
}

func (s *AIAutoBanService) getAPIHealthState() aiBanAPIHealthState {
	var state aiBanAPIHealthState
	cache.Get().GetJSON(aiBanAPIHealthKey, &state)
	return state
}

func (s *AIAutoBanService) getAPIHealthMap() map[string]interface{} {
	state := s.getAPIHealthState()
	now := time.Now().Unix()
	cooldown := int64(0)
	suspended := false
	if state.SuspendedUntil > now {
		suspended = true
		cooldown = state.SuspendedUntil - now
	}
	return map[string]interface{}{
		"suspended":            suspended,
		"consecutive_failures": state.ConsecutiveFailures,
		"last_error":           nullableString(state.LastError),
		"last_failure_at":      state.LastFailureAt,
		"suspended_until":      state.SuspendedUntil,
		"cooldown_remaining":   cooldown,
	}
}

func nullableString(v string) interface{} {
	if v == "" {
		return nil
	}
	return v
}

func (s *AIAutoBanService) resetAPIHealth() {
	cm := cache.Get()
	cm.Delete(aiBanAPIHealthKey)
	cm.Delete("ai_ban:api_paused")
}

func (s *AIAutoBanService) recordAIAPISuccess() {
	cache.Get().Set(aiBanAPIHealthKey, aiBanAPIHealthState{}, 0)
}

func (s *AIAutoBanService) recordAIAPIFailure(message string) aiBanAPIHealthState {
	state := s.getAPIHealthState()
	state.ConsecutiveFailures++
	state.LastError = message
	state.LastFailureAt = time.Now().Unix()
	if state.ConsecutiveFailures >= aiBanFailureSuspendCount {
		state.SuspendedUntil = time.Now().Add(aiBanFailureSuspendDuration).Unix()
	}
	cache.Get().Set(aiBanAPIHealthKey, state, 0)
	return state
}

func (s *AIAutoBanService) isAIAPISuspended() (bool, int64) {
	state := s.getAPIHealthState()
	now := time.Now().Unix()
	if state.SuspendedUntil > now {
		return true, state.SuspendedUntil - now
	}
	return false, 0
}

func (s *AIAutoBanService) getUserProtection(userID int64) aiBanProtection {
	if s.isWhitelistedUser(userID) {
		return aiBanProtection{Protected: true, Reason: "AI 审查白名单用户"}
	}

	row, err := s.db.QueryOne(s.db.RebindQuery(`
		SELECT id, COALESCE(username, '') as username,
			COALESCE(display_name, '') as display_name,
			COALESCE(status, 0) as status,
			COALESCE(role, 0) as role,
			deleted_at
		FROM users
		WHERE id = ?`), userID)
	if err != nil {
		return aiBanProtection{Protected: true, Reason: "读取用户信息失败: " + err.Error()}
	}
	if row == nil {
		return aiBanProtection{Protected: true, Reason: "用户不存在"}
	}
	if row["deleted_at"] != nil && toString(row["deleted_at"]) != "" {
		return aiBanProtection{Protected: true, Reason: "用户已删除", User: row}
	}
	if toInt64(row["role"]) >= 10 {
		return aiBanProtection{Protected: true, Reason: "管理员或受保护角色", User: row}
	}
	if status := toInt64(row["status"]); status != 1 {
		return aiBanProtection{Protected: true, Reason: "用户不是正常状态", User: row}
	}
	return aiBanProtection{User: row}
}

func (s *AIAutoBanService) assessUser(userID int64, window string, opts aiBanAssessOptions) (map[string]interface{}, error) {
	config := opts.config
	if config == nil {
		config = s.GetConfig()
	}
	if err := s.validateAIBanRuntimeConfig(config); err != nil {
		return s.assessmentFallback(userID, window, "config_error", err.Error()), err
	}

	seconds, ok := WindowSeconds[window]
	if !ok {
		seconds = WindowSeconds["1h"]
		window = "1h"
	}

	protection := s.getUserProtection(userID)
	username := fmt.Sprintf("用户%d", userID)
	if protection.User != nil {
		if name := toString(protection.User["username"]); name != "" {
			username = name
		}
	}
	if protection.Protected {
		return map[string]interface{}{
			"user_id":   userID,
			"username":  username,
			"window":    window,
			"protected": true,
			"skipped":   true,
			"action":    "protected",
			"message":   protection.Reason,
			"assessment": assessmentToMap(aiBanAssessment{
				ShouldBan:  false,
				RiskScore:  0,
				Confidence: 1,
				Action:     "normal",
				Reason:     protection.Reason,
			}),
		}, nil
	}

	analysis, err := (&RiskMonitoringService{db: s.db}).GetUserAnalysis(userID, seconds, nil)
	if err != nil {
		return s.assessmentFallback(userID, window, "analysis_error", "读取风险画像失败: "+err.Error()), err
	}
	if user, ok := analysis["user"].(map[string]interface{}); ok {
		if display := toString(user["display_name"]); display != "" {
			username = display
		} else if name := toString(user["username"]); name != "" {
			username = name
		}
	}

	ipHits := collectIPRuleHits(analysis, configStringSlice(config, "whitelist_ips"), configStringSlice(config, "blacklist_ips"))
	if len(ipHits["whitelist_ips"].([]string)) > 0 {
		return map[string]interface{}{
			"user_id":   userID,
			"username":  username,
			"window":    window,
			"protected": true,
			"skipped":   true,
			"action":    "monitor",
			"message":   "命中 AI IP 白名单，跳过自动封禁",
			"metrics":   buildAIBanMetrics(analysis, 0),
			"assessment": assessmentToMap(aiBanAssessment{
				ShouldBan:  false,
				RiskScore:  0,
				Confidence: 1,
				Action:     "monitor",
				Reason:     "命中 AI IP 白名单，跳过自动封禁",
			}),
		}, nil
	}

	excludedRatio, excludedStats := s.excludedRequestRatio(userID, analysis, config)
	if excludedRatio >= 0.8 {
		reason := fmt.Sprintf("排除模型/分组请求占比 %.0f%%，跳过 AI 审查", excludedRatio*100)
		return map[string]interface{}{
			"user_id":   userID,
			"username":  username,
			"window":    window,
			"protected": false,
			"skipped":   true,
			"action":    "skipped",
			"message":   reason,
			"metrics":   buildAIBanMetrics(analysis, excludedRatio),
			"excluded":  excludedStats,
			"assessment": assessmentToMap(aiBanAssessment{
				ShouldBan:  false,
				RiskScore:  0,
				Confidence: 1,
				Action:     "normal",
				Reason:     reason,
			}),
		}, nil
	}

	prompt, err := s.buildAIBanPrompt(config, analysis, ipHits, excludedStats)
	if err != nil {
		return s.assessmentFallback(userID, window, "prompt_error", err.Error()), err
	}

	start := time.Now()
	assessment, err := s.callAIBanModel(config, prompt)
	if err != nil {
		if opts.writeHealth {
			s.recordAIAPIFailure(err.Error())
		}
		failedAssessment := aiBanAssessment{
			ShouldBan:     false,
			RiskScore:     0,
			Confidence:    0,
			Action:        "monitor",
			Reason:        "AI 审查调用失败: " + err.Error(),
			APIDurationMS: time.Since(start).Milliseconds(),
		}
		var modelErr *aiBanModelError
		if errors.As(err, &modelErr) {
			failedAssessment.PromptTokens = modelErr.PromptTokens
			failedAssessment.CompletionTokens = modelErr.CompletionTokens
			failedAssessment.Model = modelErr.Model
			failedAssessment.RawResponse = modelErr.RawResponse
		}
		return map[string]interface{}{
			"user_id":    userID,
			"username":   username,
			"window":     window,
			"protected":  false,
			"skipped":    false,
			"action":     "error",
			"message":    err.Error(),
			"metrics":    buildAIBanMetrics(analysis, excludedRatio),
			"assessment": assessmentToMap(failedAssessment),
		}, err
	}
	if opts.writeHealth {
		s.recordAIAPISuccess()
	}

	assessment.APIDurationMS = time.Since(start).Milliseconds()
	normalizeAIBanAssessment(&assessment)
	action := decideAIBanAction(assessment)
	assessment.Action = action

	return map[string]interface{}{
		"user_id":        userID,
		"username":       username,
		"window":         window,
		"protected":      false,
		"skipped":        false,
		"action":         action,
		"metrics":        buildAIBanMetrics(analysis, excludedRatio),
		"excluded":       excludedStats,
		"ip_rule_hits":   ipHits,
		"assessment":     assessmentToMap(assessment),
		"assessed":       true,
		"assessed_at":    time.Now().Unix(),
		"auto_eligible":  isAutoBanEligible(assessment),
		"manual_result":  opts.manualResult,
		"analysis_range": analysis["range"],
	}, nil
}

func (s *AIAutoBanService) assessmentFallback(userID int64, window, action, reason string) map[string]interface{} {
	return map[string]interface{}{
		"user_id":  userID,
		"username": fmt.Sprintf("用户%d", userID),
		"window":   window,
		"skipped":  true,
		"action":   action,
		"message":  reason,
		"assessment": assessmentToMap(aiBanAssessment{
			ShouldBan:  false,
			RiskScore:  0,
			Confidence: 0,
			Action:     "monitor",
			Reason:     reason,
		}),
	}
}

func (s *AIAutoBanService) buildAIBanPrompt(config map[string]interface{}, analysis map[string]interface{}, ipHits map[string]interface{}, excludedStats map[string]interface{}) (string, error) {
	systemPrompt := defaultAIBanPrompt
	if custom := configString(config, "custom_prompt"); custom != "" {
		systemPrompt += "\n\n管理员补充规则：\n" + custom
	}

	payload := map[string]interface{}{
		"review_goal": "判断该用户近期调用行为是否需要封禁",
		"threshold_policy": map[string]interface{}{
			"auto_ban_requires": "should_ban=true 且 risk_score>=8 且 confidence>=0.75",
			"dry_run_note":      "系统可能处于试运行模式，AI 只负责给出风险判断",
		},
		"analysis":       compactAIBanAnalysis(analysis),
		"ip_rule_hits":   ipHits,
		"excluded_stats": excludedStats,
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return "", fmt.Errorf("构造 AI 输入失败: %w", err)
	}
	return systemPrompt + "\n\n待审查用户画像：\n" + string(data), nil
}

func compactAIBanAnalysis(analysis map[string]interface{}) map[string]interface{} {
	out := map[string]interface{}{}
	for _, key := range []string{"range", "user", "summary", "risk", "top_models", "top_channels", "top_ips"} {
		if v, ok := analysis[key]; ok {
			out[key] = v
		}
	}
	if logs, ok := analysis["recent_logs"].([]map[string]interface{}); ok {
		limit := 20
		if len(logs) < limit {
			limit = len(logs)
		}
		out["recent_logs"] = logs[:limit]
	}
	return out
}

func (s *AIAutoBanService) callAIBanModel(config map[string]interface{}, prompt string) (aiBanAssessment, error) {
	assessment, err := s.sendAIBanChatCompletion(config, prompt, true)
	if err == nil {
		return assessment, nil
	}

	jsonMode := true
	if isJSONModeUnsupported(err) {
		jsonMode = false
		assessment, err = s.sendAIBanChatCompletion(config, prompt, false)
		if err == nil {
			return assessment, nil
		}
	}

	if isAIBanParseError(err) {
		retryPrompt := prompt + "\n\n上一次回复不是合法 JSON。请重新输出一个严格 JSON 对象：所有 key 必须使用双引号，字段之间必须有逗号，不要输出 Markdown、解释、注释或多余文本。"
		retryAssessment, retryErr := s.sendAIBanChatCompletion(config, retryPrompt, jsonMode)
		if retryErr == nil {
			return retryAssessment, nil
		}
		return aiBanAssessment{}, retryErr
	}

	return aiBanAssessment{}, err
}

func (s *AIAutoBanService) sendAIBanChatCompletion(config map[string]interface{}, prompt string, jsonMode bool) (aiBanAssessment, error) {
	baseURL := configString(config, "base_url")
	apiKey := configString(config, "api_key")
	model := configString(config, "model")

	payload := map[string]interface{}{
		"model": model,
		"messages": []map[string]string{
			{"role": "system", "content": "你是严格保守的风控 JSON 审查器。必须只输出一个严格合法 JSON 对象。"},
			{"role": "user", "content": prompt},
		},
		"temperature": 0.1,
		"max_tokens":  500,
	}
	if jsonMode {
		payload["response_format"] = map[string]string{"type": "json_object"}
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return aiBanAssessment{}, fmt.Errorf("序列化 AI 请求失败: %w", err)
	}

	req, err := http.NewRequest("POST", getEndpointURL(baseURL, "/chat/completions"), bytes.NewReader(payloadBytes))
	if err != nil {
		return aiBanAssessment{}, fmt.Errorf("创建 AI 请求失败: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 45 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return aiBanAssessment{}, fmt.Errorf("AI 请求失败: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return aiBanAssessment{}, fmt.Errorf("读取 AI 响应失败: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw := trimForMessage(string(body), 1000)
		return aiBanAssessment{}, &aiBanModelError{
			Message:     fmt.Sprintf("AI 请求失败 (%d): %s", resp.StatusCode, trimForMessage(raw, 300)),
			RawResponse: raw,
			StatusCode:  resp.StatusCode,
		}
	}

	var chatResp struct {
		Model   string `json:"model"`
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Usage struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &chatResp); err != nil {
		return aiBanAssessment{}, fmt.Errorf("解析 AI 响应失败: %w", err)
	}
	if len(chatResp.Choices) == 0 {
		return aiBanAssessment{}, errors.New("AI 响应没有 choices")
	}

	assessment, err := parseAIBanAssessment(chatResp.Choices[0].Message.Content)
	if err != nil {
		return aiBanAssessment{}, &aiBanModelError{
			Message:          err.Error(),
			RawResponse:      trimForMessage(chatResp.Choices[0].Message.Content, 1000),
			PromptTokens:     chatResp.Usage.PromptTokens,
			CompletionTokens: chatResp.Usage.CompletionTokens,
			Model:            chatResp.Model,
		}
	}
	assessment.Model = chatResp.Model
	if assessment.Model == "" {
		assessment.Model = model
	}
	assessment.PromptTokens = chatResp.Usage.PromptTokens
	assessment.CompletionTokens = chatResp.Usage.CompletionTokens
	assessment.RawResponse = chatResp.Choices[0].Message.Content
	return assessment, nil
}

func isJSONModeUnsupported(err error) bool {
	var modelErr *aiBanModelError
	if !errors.As(err, &modelErr) {
		return false
	}
	if modelErr.StatusCode != http.StatusBadRequest && modelErr.StatusCode != http.StatusUnprocessableEntity {
		return false
	}
	msg := strings.ToLower(modelErr.Message + " " + modelErr.RawResponse)
	return strings.Contains(msg, "response_format") ||
		strings.Contains(msg, "json_object") ||
		strings.Contains(msg, "json mode") ||
		strings.Contains(msg, "unsupported") ||
		strings.Contains(msg, "not support")
}

func isAIBanParseError(err error) bool {
	var modelErr *aiBanModelError
	return errors.As(err, &modelErr) && modelErr.RawResponse != "" && modelErr.StatusCode == 0
}

func parseAIBanAssessment(content string) (aiBanAssessment, error) {
	jsonText := extractJSONObject(content)
	if jsonText == "" {
		return aiBanAssessment{}, errors.New("AI 响应不是有效 JSON")
	}

	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(jsonText), &raw); err != nil {
		repaired := repairAIBanJSON(jsonText)
		if repaired != jsonText {
			if repairErr := json.Unmarshal([]byte(repaired), &raw); repairErr == nil {
				return assessmentFromRaw(raw), nil
			}
		}
		if loose, ok := parseAIBanLooseFields(jsonText); ok {
			return loose, nil
		}
		return aiBanAssessment{}, fmt.Errorf("解析 AI JSON 失败: %w", err)
	}

	return assessmentFromRaw(raw), nil
}

func assessmentFromRaw(raw map[string]interface{}) aiBanAssessment {
	assessment := aiBanAssessment{
		ShouldBan:  configBool(raw, "should_ban", false),
		RiskScore:  toFloat64(raw["risk_score"]),
		Confidence: toFloat64(raw["confidence"]),
		Action:     strings.ToLower(strings.TrimSpace(toString(raw["action"]))),
		Reason:     strings.TrimSpace(toString(raw["reason"])),
	}
	if assessment.Confidence > 1 {
		assessment.Confidence = assessment.Confidence / 100
	}
	if assessment.Reason == "" {
		assessment.Reason = "AI 未提供具体理由"
	}
	if assessment.Action == "" {
		assessment.Action = "monitor"
	}
	return assessment
}

func extractJSONObject(content string) string {
	text := strings.TrimSpace(content)
	text = strings.TrimPrefix(text, "```json")
	text = strings.TrimPrefix(text, "```JSON")
	text = strings.TrimPrefix(text, "```")
	text = strings.TrimSuffix(text, "```")
	text = strings.TrimSpace(text)
	if strings.HasPrefix(text, "{") && strings.HasSuffix(text, "}") {
		return text
	}
	start := strings.Index(text, "{")
	end := strings.LastIndex(text, "}")
	if start >= 0 && end > start {
		return text[start : end+1]
	}
	return ""
}

func repairAIBanJSON(text string) string {
	repaired := strings.NewReplacer(
		"“", "\"",
		"”", "\"",
		"‘", "'",
		"’", "'",
	).Replace(strings.TrimSpace(text))
	repaired = regexp.MustCompile(`'([^'\\]*(?:\\.[^'\\]*)*)'`).ReplaceAllString(repaired, `"$1"`)
	repaired = regexp.MustCompile(`([,{]\s*)([A-Za-z_][A-Za-z0-9_]*)\s*:`).ReplaceAllString(repaired, `$1"$2":`)
	repaired = regexp.MustCompile(`,\s*([}\]])`).ReplaceAllString(repaired, `$1`)
	repaired = insertMissingJSONCommas(repaired)
	return repaired
}

func insertMissingJSONCommas(text string) string {
	valueRE := regexp.MustCompile(`(?s)(true|false|null|-?\d+(?:\.\d+)?|"[^"\\]*(?:\\.[^"\\]*)*")(\s+)("[A-Za-z_][A-Za-z0-9_]*"\s*:)`)
	previous := ""
	current := text
	for previous != current {
		previous = current
		current = valueRE.ReplaceAllString(current, `$1,$3`)
	}
	return current
}

func parseAIBanLooseFields(text string) (aiBanAssessment, bool) {
	normalized := repairAIBanJSON(text)
	assessment := aiBanAssessment{
		ShouldBan:  looseBoolField(normalized, "should_ban"),
		RiskScore:  looseNumberField(normalized, "risk_score"),
		Confidence: looseNumberField(normalized, "confidence"),
		Action:     strings.ToLower(strings.TrimSpace(looseStringField(normalized, "action"))),
		Reason:     strings.TrimSpace(looseStringField(normalized, "reason")),
	}
	if assessment.Confidence > 1 {
		assessment.Confidence = assessment.Confidence / 100
	}
	if assessment.Reason == "" {
		assessment.Reason = "AI 返回了非标准 JSON，已按字段容错解析"
	}
	if assessment.Action == "" {
		assessment.Action = "monitor"
	}
	hasSignal := regexp.MustCompile(`(?i)["']?(should_ban|risk_score|confidence|action|reason)["']?\s*:`).MatchString(normalized)
	return assessment, hasSignal
}

func looseBoolField(text, key string) bool {
	re := regexp.MustCompile(`(?i)["']?` + regexp.QuoteMeta(key) + `["']?\s*:\s*(true|false)`)
	match := re.FindStringSubmatch(text)
	return len(match) > 1 && strings.EqualFold(match[1], "true")
}

func looseNumberField(text, key string) float64 {
	re := regexp.MustCompile(`(?i)["']?` + regexp.QuoteMeta(key) + `["']?\s*:\s*([-+]?\d+(?:\.\d+)?)\s*%?`)
	match := re.FindStringSubmatch(text)
	if len(match) <= 1 {
		return 0
	}
	value, _ := strconv.ParseFloat(match[1], 64)
	return value
}

func looseStringField(text, key string) string {
	quoted := regexp.MustCompile(`(?is)["']?` + regexp.QuoteMeta(key) + `["']?\s*:\s*["']([^"']+)["']`)
	if match := quoted.FindStringSubmatch(text); len(match) > 1 {
		return match[1]
	}
	unquoted := regexp.MustCompile(`(?im)["']?` + regexp.QuoteMeta(key) + `["']?\s*:\s*([^,\n\r}]+)`)
	if match := unquoted.FindStringSubmatch(text); len(match) > 1 {
		return strings.TrimSpace(match[1])
	}
	return ""
}

func normalizeAIBanAssessment(a *aiBanAssessment) {
	if a.RiskScore < 0 {
		a.RiskScore = 0
	}
	if a.RiskScore > 10 {
		a.RiskScore = 10
	}
	if a.Confidence < 0 {
		a.Confidence = 0
	}
	if a.Confidence > 1 {
		a.Confidence = 1
	}
	a.ModelShouldBan = a.ShouldBan
	if !isAutoBanEligible(*a) && a.Action == "ban" {
		if a.RiskScore >= 5 {
			a.Action = "warn"
		} else {
			a.Action = "monitor"
		}
	}
	a.ShouldBan = isAutoBanEligible(*a)
}

func isAutoBanEligible(a aiBanAssessment) bool {
	return a.ShouldBan && a.RiskScore >= aiBanAutoBanRiskScore && a.Confidence >= aiBanAutoBanConfidence
}

func decideAIBanAction(a aiBanAssessment) string {
	switch a.Action {
	case "normal", "monitor", "warn", "ban":
	default:
		a.Action = ""
	}
	if isAutoBanEligible(a) {
		return "ban"
	}
	if a.Action == "warn" || a.RiskScore >= 7 {
		return "warn"
	}
	if a.Action == "monitor" || a.RiskScore >= 4 {
		return "monitor"
	}
	return "normal"
}

func assessmentToMap(a aiBanAssessment) map[string]interface{} {
	result := map[string]interface{}{
		"should_ban":        a.ShouldBan,
		"model_should_ban":  a.ModelShouldBan,
		"risk_score":        a.RiskScore,
		"confidence":        a.Confidence,
		"action":            a.Action,
		"reason":            a.Reason,
		"prompt_tokens":     a.PromptTokens,
		"completion_tokens": a.CompletionTokens,
		"api_duration_ms":   a.APIDurationMS,
		"model":             a.Model,
	}
	if a.RawResponse != "" {
		result["raw_response"] = trimForMessage(a.RawResponse, 1000)
	}
	return result
}

func buildAIBanMetrics(analysis map[string]interface{}, excludedRatio float64) map[string]interface{} {
	metrics := map[string]interface{}{
		"excluded_ratio": math.Round(excludedRatio*10000) / 100,
	}
	if summary, ok := analysis["summary"].(map[string]interface{}); ok {
		metrics["summary"] = summary
	}
	if risk, ok := analysis["risk"].(map[string]interface{}); ok {
		metrics["risk"] = risk
	}
	return metrics
}

func (s *AIAutoBanService) excludedRequestRatio(userID int64, analysis map[string]interface{}, config map[string]interface{}) (float64, map[string]interface{}) {
	models := configStringSlice(config, "excluded_models")
	groups := configStringSlice(config, "excluded_groups")
	stats := map[string]interface{}{
		"total_requests":    0,
		"excluded_requests": 0,
		"excluded_ratio":    0.0,
		"excluded_models":   models,
		"excluded_groups":   groups,
	}
	if len(models) == 0 && len(groups) == 0 {
		return 0, stats
	}

	rangeMap, _ := analysis["range"].(map[string]interface{})
	startTime := toInt64(rangeMap["start_time"])
	endTime := toInt64(rangeMap["end_time"])
	if startTime == 0 || endTime == 0 {
		return 0, stats
	}

	conditions := []string{}
	args := []interface{}{userID, startTime, endTime}
	if len(models) > 0 {
		conditions = append(conditions, fmt.Sprintf("model_name IN (%s)", buildPlaceholders(false, len(models), 1)))
		for _, m := range models {
			args = append(args, m)
		}
	}
	if len(groups) > 0 {
		conditions = append(conditions, fmt.Sprintf("%s IN (%s)", s.groupCol(), buildPlaceholders(false, len(groups), 1)))
		for _, g := range groups {
			args = append(args, g)
		}
	}

	query := fmt.Sprintf(`
		SELECT COUNT(*) as total_requests,
			SUM(CASE WHEN %s THEN 1 ELSE 0 END) as excluded_requests
		FROM logs
		WHERE user_id = ? AND created_at >= ? AND created_at <= ? AND type IN (2, 5)`,
		strings.Join(conditions, " OR "))
	query = s.db.RebindQuery(query)
	row, err := s.db.QueryOne(query, args...)
	if err != nil || row == nil {
		return 0, stats
	}
	total := toInt64(row["total_requests"])
	excluded := toInt64(row["excluded_requests"])
	ratio := 0.0
	if total > 0 {
		ratio = float64(excluded) / float64(total)
	}
	stats["total_requests"] = total
	stats["excluded_requests"] = excluded
	stats["excluded_ratio"] = ratio
	return ratio, stats
}

func collectIPRuleHits(analysis map[string]interface{}, whitelist, blacklist []string) map[string]interface{} {
	topIPs, _ := analysis["top_ips"].([]map[string]interface{})
	wlHits := []string{}
	blHits := []string{}
	for _, row := range topIPs {
		ip := toString(row["ip"])
		if ip == "" {
			continue
		}
		if matchIPRuleList(ip, whitelist) {
			wlHits = append(wlHits, ip)
		}
		if matchIPRuleList(ip, blacklist) {
			blHits = append(blHits, ip)
		}
	}
	sort.Strings(wlHits)
	sort.Strings(blHits)
	return map[string]interface{}{
		"whitelist_ips": wlHits,
		"blacklist_ips": blHits,
	}
}

func matchIPRuleList(ip string, rules []string) bool {
	parsed := net.ParseIP(ip)
	for _, rule := range rules {
		rule = strings.TrimSpace(rule)
		if rule == "" {
			continue
		}
		if rule == ip {
			return true
		}
		if _, network, err := net.ParseCIDR(rule); err == nil && parsed != nil && network.Contains(parsed) {
			return true
		}
	}
	return false
}

func (s *AIAutoBanService) appendAuditLog(entry map[string]interface{}) map[string]interface{} {
	cm := cache.Get()
	var logs []map[string]interface{}
	cm.GetJSON(aiBanAuditLogsKey, &logs)

	maxID := int64(0)
	for _, log := range logs {
		if id := toInt64(log["id"]); id > maxID {
			maxID = id
		}
	}
	entry["id"] = maxID + 1
	if entry["created_at"] == nil {
		entry["created_at"] = time.Now().Unix()
	}

	logs = append([]map[string]interface{}{entry}, logs...)
	if len(logs) > aiBanAuditLogLimit {
		logs = logs[:aiBanAuditLogLimit]
	}
	cm.Set(aiBanAuditLogsKey, logs, 0)
	return entry
}

func (s *AIAutoBanService) buildAuditLog(scanID, status, window string, dryRun bool, started time.Time, details []map[string]interface{}, errMsg string) map[string]interface{} {
	bannedCount := 0
	warnedCount := 0
	skippedCount := 0
	errorCount := 0
	processed := 0
	for _, detail := range details {
		action := toString(detail["action"])
		switch action {
		case "ban":
			bannedCount++
			processed++
		case "warn":
			warnedCount++
			processed++
		case "error":
			errorCount++
		case "normal", "monitor":
			skippedCount++
			processed++
		default:
			skippedCount++
		}
	}

	return map[string]interface{}{
		"scan_id":         scanID,
		"status":          status,
		"window":          window,
		"total_scanned":   len(details),
		"total_processed": processed,
		"banned_count":    bannedCount,
		"warned_count":    warnedCount,
		"skipped_count":   skippedCount,
		"error_count":     errorCount,
		"dry_run":         dryRun,
		"elapsed_seconds": math.Round(time.Since(started).Seconds()*100) / 100,
		"error_message":   errMsg,
		"details":         details,
		"created_at":      time.Now().Unix(),
	}
}

func (s *AIAutoBanService) RunScheduledScan() map[string]interface{} {
	config := s.GetConfig()
	if !configBool(config, "enabled", false) {
		return map[string]interface{}{"skipped": true, "message": "AI 审查未启用"}
	}
	interval := configInt(config, "scan_interval_minutes", 0)
	if interval <= 0 {
		return map[string]interface{}{"skipped": true, "message": "AI 定时扫描未启用"}
	}

	cm := cache.Get()
	var lastScan int64
	cm.GetJSON(aiBanLastScanKey, &lastScan)
	now := time.Now().Unix()
	if lastScan > 0 && now-lastScan < interval*60 {
		return map[string]interface{}{"skipped": true, "message": "未到下次扫描时间"}
	}

	result := s.RunScan("1h", 20)
	cm.Set(aiBanLastScanKey, now, 0)
	return result
}

func trimForMessage(s string, max int) string {
	s = strings.TrimSpace(s)
	if len(s) <= max {
		return s
	}
	return s[:max]
}

func mathRound(v float64, places int) float64 {
	factor := math.Pow10(places)
	return math.Round(v*factor) / factor
}

func classifyAIBanStatus(details []map[string]interface{}) string {
	if len(details) == 0 {
		return "empty"
	}
	errors := 0
	for _, detail := range details {
		if toString(detail["action"]) == "error" {
			errors++
		}
	}
	if errors == 0 {
		return "success"
	}
	if errors == len(details) {
		return "failed"
	}
	return "partial"
}

func (s *AIAutoBanService) executeAutoBanIfNeeded(detail map[string]interface{}, dryRun bool) {
	if dryRun || toString(detail["action"]) != "ban" {
		detail["executed"] = false
		return
	}
	userID := toInt64(detail["user_id"])
	if userID <= 0 {
		detail["action"] = "error"
		detail["message"] = "无效用户 ID，无法自动封禁"
		detail["executed"] = false
		return
	}
	if err := (&UserManagementService{db: s.db}).BanUser(userID, true); err != nil {
		detail["action"] = "error"
		detail["message"] = "自动封禁失败: " + err.Error()
		detail["executed"] = false
		return
	}
	detail["executed"] = true
	detail["message"] = "已自动封禁并禁用 Token"
	logger.L.Security(fmt.Sprintf("AI 自动封禁 user_id=%d reason=%s", userID, detailReason(detail)))
}

func detailReason(detail map[string]interface{}) string {
	assessment, _ := detail["assessment"].(map[string]interface{})
	return toString(assessment["reason"])
}
