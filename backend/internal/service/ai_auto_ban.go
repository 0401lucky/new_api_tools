package service

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/new-api-tools/backend/internal/cache"
	"github.com/new-api-tools/backend/internal/database"
)

// AIAutoBanService handles AI-assisted automatic user banning
type AIAutoBanService struct {
	db *database.Manager
}

// NewAIAutoBanService creates a new AIAutoBanService
func NewAIAutoBanService() *AIAutoBanService {
	return &AIAutoBanService{db: database.Get()}
}

// Default config
var defaultAIBanConfig = map[string]interface{}{
	"base_url":              "",
	"api_key":               "",
	"model":                 "",
	"enabled":               false,
	"dry_run":               true,
	"scan_interval_minutes": 30,
	"scan_window":           "24h",
	"scan_limit":            50,
	"risk_score_threshold":  8.0,
	"confidence_threshold":  0.75,
	"custom_prompt":         "",
	"whitelist_ips":         []string{},
	"blacklist_ips":         []string{},
	"excluded_models":       []string{},
	"excluded_groups":       []string{},
}

// GetConfig returns AI auto ban configuration with computed fields
func (s *AIAutoBanService) GetConfig() map[string]interface{} {
	config := s.getRawAIBanConfig()

	// Compute has_api_key and masked_api_key (matching Python backend behavior)
	apiKey := configString(config, "api_key")
	config["has_api_key"] = apiKey != ""
	config["masked_api_key"] = maskAIBanAPIKey(apiKey)
	delete(config, "api_key")

	config["api_health"] = s.getAPIHealthMap()
	config["default_prompt"] = defaultAIBanPrompt
	config["whitelist_count"] = len(s.getWhitelistIDs())

	return config
}

// SaveConfig saves AI auto ban configuration
func (s *AIAutoBanService) SaveConfig(updates map[string]interface{}) error {
	config := s.getRawAIBanConfig()
	currentAPIKey := configString(config, "api_key")
	maskedAPIKey := maskAIBanAPIKey(currentAPIKey)

	// Apply updates
	for k, v := range updates {
		if k == "api_key" {
			nextAPIKey := strings.TrimSpace(toString(v))
			if nextAPIKey == "" || (maskedAPIKey != "" && nextAPIKey == maskedAPIKey) {
				continue
			}
			config[k] = nextAPIKey
			continue
		}
		config[k] = v
	}

	// Strip computed fields before saving (they are re-computed in GetConfig)
	stripComputedAIBanConfig(config)
	normalizeAIBanConfig(config)

	return s.saveAIBanJSON(aiBanConfigKey, config)
}

func maskAIBanAPIKey(apiKey string) string {
	if apiKey == "" {
		return ""
	}
	if len(apiKey) > 8 {
		return apiKey[:4] + strings.Repeat("*", len(apiKey)-8) + apiKey[len(apiKey)-4:]
	}
	return strings.Repeat("*", len(apiKey))
}

func stripComputedAIBanConfig(config map[string]interface{}) {
	delete(config, "has_api_key")
	delete(config, "masked_api_key")
	delete(config, "api_health")
	delete(config, "default_prompt")
	delete(config, "whitelist_count")
}

func (s *AIAutoBanService) loadAIBanJSON(key string, dest interface{}) bool {
	cm := cache.Get()
	if found, err := cm.GetJSON(key, dest); found && err == nil {
		return true
	}
	if s == nil || s.db == nil {
		return false
	}

	keyCol := "`key`"
	if s.db.IsPG {
		keyCol = `"key"`
	}
	query := s.db.RebindQuery(fmt.Sprintf("SELECT value FROM options WHERE %s = ?", keyCol))
	row, err := s.db.QueryOne(query, key)
	if err != nil || row == nil {
		return false
	}
	value := toString(row["value"])
	if value == "" {
		return false
	}
	if err := json.Unmarshal([]byte(value), dest); err != nil {
		return false
	}
	_ = cm.Set(key, dest, 0)
	return true
}

func (s *AIAutoBanService) saveAIBanJSON(key string, value interface{}) error {
	err := cache.Get().Set(key, value, 0)
	_ = s.persistAIBanJSON(key, value)
	return err
}

func (s *AIAutoBanService) persistAIBanJSON(key string, value interface{}) error {
	if s == nil || s.db == nil {
		return nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if s.db.IsPG {
		query := s.db.RebindQuery(`
			INSERT INTO options ("key", value)
			VALUES (?, ?)
			ON CONFLICT ("key") DO UPDATE SET value = EXCLUDED.value`)
		_, err = s.db.Execute(query, key, string(data))
		return err
	}
	query := s.db.RebindQuery(`
		INSERT INTO options (` + "`key`" + `, value)
		VALUES (?, ?)
		ON DUPLICATE KEY UPDATE value = VALUES(value)`)
	_, err = s.db.Execute(query, key, string(data))
	return err
}

// ResetAPIHealth resets the API health status
func (s *AIAutoBanService) ResetAPIHealth() map[string]interface{} {
	s.resetAPIHealth()
	return map[string]interface{}{
		"message": "API 健康状态已重置",
		"status":  "healthy",
	}
}

// GetAuditLogs returns AI audit logs
func (s *AIAutoBanService) GetAuditLogs(limit, offset int, status string) map[string]interface{} {
	var allLogs []map[string]interface{}
	s.loadAIBanJSON(aiBanAuditLogsKey, &allLogs)

	// Filter by status if provided
	filtered := allLogs
	if status != "" {
		filtered = make([]map[string]interface{}, 0)
		for _, log := range allLogs {
			if logStatus, ok := log["status"].(string); ok && logStatus == status {
				filtered = append(filtered, log)
			}
		}
	}

	total := len(filtered)
	// Paginate
	start := offset
	end := offset + limit
	if start > total {
		start = total
	}
	if end > total {
		end = total
	}

	return map[string]interface{}{
		"items":  filtered[start:end],
		"total":  total,
		"limit":  limit,
		"offset": offset,
	}
}

// ClearAuditLogs clears all AI audit logs
func (s *AIAutoBanService) ClearAuditLogs() map[string]interface{} {
	_ = s.saveAIBanJSON(aiBanAuditLogsKey, []map[string]interface{}{})
	return map[string]interface{}{
		"message": "审查记录已清空",
	}
}

// groupCol returns the properly quoted column name for 'group' (reserved word)
func (s *AIAutoBanService) groupCol() string {
	if s.db.IsPG {
		return `"group"`
	}
	return "`group`"
}

// GetAvailableGroups returns groups used in recent logs
func (s *AIAutoBanService) GetAvailableGroups(days int) ([]map[string]interface{}, error) {
	startTime := time.Now().Unix() - int64(days*86400)
	groupCol := s.groupCol()
	query := s.db.RebindQuery(fmt.Sprintf(`
		SELECT %s as name, %s as group_name, COUNT(*) as count, COUNT(*) as requests
		FROM logs
		WHERE created_at >= ? AND %s IS NOT NULL AND %s != ''
		GROUP BY %s
		ORDER BY count DESC`, groupCol, groupCol, groupCol, groupCol, groupCol))

	rows, err := s.db.Query(query, startTime)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// GetAvailableModelsForExclude returns models used in recent logs
func (s *AIAutoBanService) GetAvailableModelsForExclude(days int) ([]map[string]interface{}, error) {
	startTime := time.Now().Unix() - int64(days*86400)
	query := s.db.RebindQuery(`
		SELECT model_name as name, model_name as model_name, COUNT(*) as count, COUNT(*) as requests
		FROM logs
		WHERE created_at >= ? AND model_name IS NOT NULL AND model_name != ''
		GROUP BY model_name
		ORDER BY count DESC`)

	rows, err := s.db.Query(query, startTime)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// GetSuspiciousUsers returns users with suspicious behavior patterns
func (s *AIAutoBanService) GetSuspiciousUsers(window string, limit int) ([]map[string]interface{}, error) {
	seconds, ok := WindowSeconds[window]
	if !ok {
		seconds = 3600
	}
	startTime := time.Now().Unix() - seconds

	config := s.getRawAIBanConfig()
	blacklistIPs := configStringSlice(config, "blacklist_ips")
	cacheKey := fmt.Sprintf("ai_ban:suspicious:%s:%d:%s", window, limit, strings.Join(blacklistIPs, ","))
	cm := cache.Get()
	var cached []map[string]interface{}
	found, _ := cm.GetJSON(cacheKey, &cached)
	if found {
		return cached, nil
	}

	windowMinutes := float64(seconds) / 60.0
	rawLimit := limit * 10
	if rawLimit < 50 {
		rawLimit = 50
	}
	if rawLimit > 500 {
		rawLimit = 500
	}

	// Find a broad candidate pool, then score richer signals in Go.
	query := s.db.RebindQuery(`
		SELECT l.user_id, COALESCE(MAX(NULLIF(u.display_name, '')), MAX(NULLIF(u.username, '')), MAX(NULLIF(l.username, '')), '') as username,
			COUNT(*) as total_requests,
			SUM(CASE WHEN l.type = 5 THEN 1 ELSE 0 END) as failure_count,
			SUM(CASE WHEN l.type = 2 AND COALESCE(l.completion_tokens, 0) = 0 THEN 1 ELSE 0 END) as empty_count,
			COALESCE(SUM(l.quota), 0) as total_quota,
			COUNT(DISTINCT NULLIF(l.ip, '')) as unique_ips,
			COUNT(DISTINCT NULLIF(l.model_name, '')) as unique_models,
			COUNT(DISTINCT l.token_id) as unique_tokens
		FROM logs l
		LEFT JOIN users u ON l.user_id = u.id
		WHERE l.created_at >= ? AND l.type IN (2, 5) AND l.user_id IS NOT NULL
			AND (u.deleted_at IS NULL)
			AND COALESCE(u.status, 1) = 1
			AND COALESCE(u.role, 0) < 10
		GROUP BY l.user_id
		HAVING COUNT(*) >= 10
		ORDER BY failure_count DESC, unique_ips DESC, total_requests DESC
		LIMIT ?`)

	rows, err := s.db.Query(query, startTime, rawLimit)
	if err != nil {
		return nil, err
	}

	for _, row := range rows {
		s.enrichAIBanCandidate(row, startTime, windowMinutes, blacklistIPs)
	}

	sort.SliceStable(rows, func(i, j int) bool {
		leftScore := toFloat64(rows[i]["suspicion_score"])
		rightScore := toFloat64(rows[j]["suspicion_score"])
		if leftScore != rightScore {
			return leftScore > rightScore
		}
		leftFailure := toFloat64(rows[i]["failure_rate"])
		rightFailure := toFloat64(rows[j]["failure_rate"])
		if leftFailure != rightFailure {
			return leftFailure > rightFailure
		}
		leftRapid := toInt64(rows[i]["rapid_switch_count"])
		rightRapid := toInt64(rows[j]["rapid_switch_count"])
		if leftRapid != rightRapid {
			return leftRapid > rightRapid
		}
		return toInt64(rows[i]["total_requests"]) > toInt64(rows[j]["total_requests"])
	})
	if len(rows) > limit {
		rows = rows[:limit]
	}

	cm.Set(cacheKey, rows, 2*time.Minute)
	return rows, nil
}

func (s *AIAutoBanService) enrichAIBanCandidate(row map[string]interface{}, startTime int64, windowMinutes float64, blacklistIPs []string) {
	total := toInt64(row["total_requests"])
	failures := toInt64(row["failure_count"])
	emptyCount := toInt64(row["empty_count"])
	uniqueIPs := toInt64(row["unique_ips"])
	uniqueTokens := toInt64(row["unique_tokens"])

	failureRate := 0.0
	emptyRate := 0.0
	rpm := 0.0
	if total > 0 {
		failureRate = float64(failures) / float64(total) * 100
		emptyRate = float64(emptyCount) / float64(total) * 100
	}
	if windowMinutes > 0 {
		rpm = mathRound(float64(total)/windowMinutes, 2)
	}

	ipSequence := s.getAIBanIPSequence(toInt64(row["user_id"]), startTime, 500)
	ipSwitchAnalysis := analyzeIPSwitches(ipSequence)
	rapidSwitchCount := toInt64(ipSwitchAnalysis["rapid_switch_count"])
	realSwitchCount := toInt64(ipSwitchAnalysis["real_switch_count"])
	avgIPDuration := toFloat64(ipSwitchAnalysis["avg_ip_duration"])
	blacklistHits := collectIPHitsFromSequence(ipSequence, blacklistIPs)

	flags := []string{}
	score := 0.0
	if failureRate >= 50 && total >= 10 {
		flags = append(flags, "HIGH_FAILURE_RATE")
		score += 40
	}
	if emptyRate >= 80 && total >= 10 {
		flags = append(flags, "EMPTY_RESPONSE_ABUSE")
		score += 35
	}
	if uniqueIPs > 10 {
		flags = append(flags, "MANY_IPS")
		score += 25
	}
	if rpm > 5 {
		flags = append(flags, "HIGH_RPM")
		score += 20
	}
	if rapidSwitchCount >= 3 && avgIPDuration < 300 {
		flags = append(flags, "IP_RAPID_SWITCH")
		score += 25
	}
	if avgIPDuration < 30 && realSwitchCount >= 3 {
		flags = append(flags, "IP_HOPPING")
		score += 30
	}
	if uniqueTokens >= 5 && total > 0 && float64(total)/float64(uniqueTokens) <= 3 {
		flags = append(flags, "TOKEN_ROTATION")
		score += 20
	}
	if len(blacklistHits) > 0 {
		flags = append(flags, "BLACKLIST_IP")
		score += 40
	}
	if total >= 50 {
		score += 5
	}

	row["failure_rate"] = mathRound(failureRate, 2)
	row["empty_rate"] = mathRound(emptyRate, 2)
	row["rpm"] = rpm
	row["risk_flags"] = flags
	row["rapid_switch_count"] = rapidSwitchCount
	row["ip_switch_analysis"] = ipSwitchAnalysis
	row["blacklist_ips"] = blacklistHits
	row["blacklist_hit_count"] = len(blacklistHits)
	row["suspicion_score"] = mathRound(score, 2)
}

func (s *AIAutoBanService) getAIBanWindowSummary(userID int64, window string, seconds int64, blacklistIPs []string) map[string]interface{} {
	startTime := time.Now().Unix() - seconds
	query := s.db.RebindQuery(`
		SELECT COUNT(*) as total_requests,
			SUM(CASE WHEN type = 5 THEN 1 ELSE 0 END) as failure_count,
			SUM(CASE WHEN type = 2 AND COALESCE(completion_tokens, 0) = 0 THEN 1 ELSE 0 END) as empty_count,
			COALESCE(SUM(quota), 0) as total_quota,
			COUNT(DISTINCT NULLIF(ip, '')) as unique_ips,
			COUNT(DISTINCT NULLIF(model_name, '')) as unique_models,
			COUNT(DISTINCT token_id) as unique_tokens
		FROM logs
		WHERE user_id = ? AND created_at >= ? AND type IN (2, 5)`)
	row, err := s.db.QueryOneWithTimeout(10*time.Second, query, userID, startTime)
	if err != nil || row == nil {
		row = map[string]interface{}{}
	}

	total := toInt64(row["total_requests"])
	failures := toInt64(row["failure_count"])
	emptyCount := toInt64(row["empty_count"])
	failureRate := 0.0
	emptyRate := 0.0
	rpm := 0.0
	if total > 0 {
		failureRate = float64(failures) / float64(total) * 100
		emptyRate = float64(emptyCount) / float64(total) * 100
		rpm = float64(total) / (float64(seconds) / 60.0)
	}

	blacklistHits := collectIPHitsFromSequence(s.getAIBanIPSequence(userID, startTime, 1000), blacklistIPs)
	return map[string]interface{}{
		"window":              window,
		"total_requests":      total,
		"failure_count":       failures,
		"empty_count":         emptyCount,
		"total_quota":         toInt64(row["total_quota"]),
		"unique_ips":          toInt64(row["unique_ips"]),
		"unique_models":       toInt64(row["unique_models"]),
		"unique_tokens":       toInt64(row["unique_tokens"]),
		"failure_rate":        mathRound(failureRate, 2),
		"empty_rate":          mathRound(emptyRate, 2),
		"rpm":                 mathRound(rpm, 2),
		"blacklist_hit_count": len(blacklistHits),
		"blacklist_ips":       blacklistHits,
	}
}

func (s *AIAutoBanService) getAIBanIPSequence(userID int64, startTime int64, limit int) []map[string]interface{} {
	if userID <= 0 {
		return []map[string]interface{}{}
	}
	query := s.db.RebindQuery(`
		SELECT created_at, ip
		FROM logs
		WHERE user_id = ? AND created_at >= ?
			AND type IN (2, 5) AND ip IS NOT NULL AND ip != ''
		ORDER BY created_at ASC
		LIMIT ?`)
	rows, err := s.db.QueryWithTimeout(10*time.Second, query, userID, startTime, limit)
	if err != nil || rows == nil {
		return []map[string]interface{}{}
	}
	return rows
}

func collectIPHitsFromSequence(ipSequence []map[string]interface{}, rules []string) []string {
	if len(rules) == 0 {
		return []string{}
	}
	hits := map[string]bool{}
	for _, row := range ipSequence {
		ip := toString(row["ip"])
		if ip != "" && matchIPRuleList(ip, rules) {
			hits[ip] = true
		}
	}
	result := make([]string, 0, len(hits))
	for ip := range hits {
		result = append(result, ip)
	}
	sort.Strings(result)
	return result
}

// ManualAssess performs AI assessment on a single user.
func (s *AIAutoBanService) ManualAssess(userID int64, window string) map[string]interface{} {
	result, _ := s.assessUser(userID, window, aiBanAssessOptions{
		source:       "manual",
		writeHealth:  true,
		manualResult: true,
	})
	return result
}

// RunScan performs an AI assessment scan.
func (s *AIAutoBanService) RunScan(window string, limit int) map[string]interface{} {
	started := time.Now()
	scanID := fmt.Sprintf("scan_%d", started.UnixMilli())
	config := s.getRawAIBanConfig()
	dryRun := configBool(config, "dry_run", true)

	finish := func(status string, details []map[string]interface{}, errMsg string) map[string]interface{} {
		audit := s.appendAuditLog(s.buildAuditLog(scanID, status, window, dryRun, started, details, errMsg))
		return map[string]interface{}{
			"stats":     audit,
			"audit_log": audit,
			"message":   errMsg,
		}
	}

	if !configBool(config, "enabled", false) {
		return finish("skipped", nil, "AI 审查未启用")
	}
	if err := s.validateAIBanRuntimeConfig(config); err != nil {
		return finish("failed", nil, err.Error())
	}
	if suspended, remaining := s.isAIAPISuspended(); suspended {
		return finish("suspended", nil, fmt.Sprintf("AI API 已暂停，剩余冷却 %d 秒", remaining))
	}
	if !aiBanScanMu.TryLock() {
		return finish("skipped", nil, "已有 AI 审查任务正在运行")
	}
	defer aiBanScanMu.Unlock()

	users, err := s.GetSuspiciousUsers(window, limit)
	if err != nil {
		return finish("failed", nil, "获取可疑用户失败: "+err.Error())
	}
	if len(users) == 0 {
		return finish("empty", nil, "")
	}

	details := make([]map[string]interface{}, 0, len(users))
	for _, user := range users {
		userID := toInt64(user["user_id"])
		if userID <= 0 {
			details = append(details, map[string]interface{}{
				"action":  "error",
				"message": "无效用户 ID",
			})
			continue
		}
		detail, _ := s.assessUser(userID, window, aiBanAssessOptions{
			config:      config,
			source:      "scan",
			writeHealth: true,
		})
		s.executeAutoBanIfNeeded(detail, dryRun)
		details = append(details, detail)
	}

	return finish(classifyAIBanStatus(details), details, "")
}

// TestConnection tests the configured API connection.
func (s *AIAutoBanService) TestConnection() map[string]interface{} {
	config := s.getRawAIBanConfig()
	baseURL, _ := config["base_url"].(string)
	if baseURL == "" {
		return map[string]interface{}{
			"success": false,
			"message": "未配置 API Base URL",
		}
	}
	apiKey, _ := config["api_key"].(string)
	model, _ := config["model"].(string)
	if apiKey == "" {
		return map[string]interface{}{
			"success": false,
			"message": "API Key 未配置",
		}
	}
	if model == "" {
		return s.FetchModels(baseURL, apiKey, true)
	}
	return s.TestModel(baseURL, apiKey, model)
}

// getEndpointURL builds the API URL, auto-appending /v1 if needed
func getEndpointURL(baseURL, endpoint string) string {
	base := strings.TrimRight(baseURL, "/")
	if strings.HasSuffix(base, "/v1") {
		return base + endpoint
	}
	return base + "/v1" + endpoint
}

// FetchModels fetches available models from OpenAI-compatible /v1/models API with caching
func (s *AIAutoBanService) FetchModels(baseURL, apiKey string, forceRefresh bool) map[string]interface{} {
	config := s.getRawAIBanConfig()

	if baseURL == "" {
		baseURL, _ = config["base_url"].(string)
	}
	base := strings.TrimRight(baseURL, "/")

	if apiKey == "" {
		apiKey, _ = config["api_key"].(string)
	}
	if apiKey == "" {
		return map[string]interface{}{
			"success": false,
			"message": "API Key 未配置",
			"models":  []interface{}{},
		}
	}

	cm := cache.Get()
	cacheKey := "ai_ban:models_cache"
	cacheURLKey := "ai_ban:models_cache_url"

	// Check if API URL changed
	var cachedURL string
	if found, _ := cm.GetJSON(cacheURLKey, &cachedURL); found && cachedURL != base {
		forceRefresh = true
	}

	// Check cache (permanent, 30 days TTL)
	if !forceRefresh {
		var cached []map[string]interface{}
		if found, _ := cm.GetJSON(cacheKey, &cached); found && len(cached) > 0 {
			return map[string]interface{}{
				"success": true,
				"message": fmt.Sprintf("获取到 %d 个模型", len(cached)),
				"models":  cached,
			}
		}
	}

	// Call external API
	url := getEndpointURL(base, "/models")
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return map[string]interface{}{
			"success": false,
			"message": fmt.Sprintf("创建请求失败: %s", err.Error()),
			"models":  []interface{}{},
		}
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		msg := "连接失败，请检查 API 地址"
		if strings.Contains(err.Error(), "timeout") {
			msg = "请求超时，请检查网络或 API 地址"
		}
		return map[string]interface{}{
			"success": false,
			"message": msg,
			"models":  []interface{}{},
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return map[string]interface{}{
			"success": false,
			"message": fmt.Sprintf("请求失败: %d", resp.StatusCode),
			"models":  []interface{}{},
		}
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return map[string]interface{}{
			"success": false,
			"message": fmt.Sprintf("读取响应失败: %s", err.Error()),
			"models":  []interface{}{},
		}
	}

	var data struct {
		Data []struct {
			ID      string `json:"id"`
			OwnedBy string `json:"owned_by"`
			Created int64  `json:"created"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &data); err != nil {
		return map[string]interface{}{
			"success": false,
			"message": fmt.Sprintf("解析响应失败: %s", err.Error()),
			"models":  []interface{}{},
		}
	}

	// Build model list
	models := make([]map[string]interface{}, 0, len(data.Data))
	for _, m := range data.Data {
		if m.ID != "" {
			models = append(models, map[string]interface{}{
				"id":       m.ID,
				"owned_by": m.OwnedBy,
				"created":  m.Created,
			})
		}
	}

	// Sort by model ID
	sort.Slice(models, func(i, j int) bool {
		return models[i]["id"].(string) < models[j]["id"].(string)
	})

	// Cache permanently (30 days TTL)
	cacheTTL := 30 * 24 * time.Hour
	cm.Set(cacheKey, models, cacheTTL)
	cm.Set(cacheURLKey, base, cacheTTL)

	return map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("获取到 %d 个模型", len(models)),
		"models":  models,
	}
}

// TestModel tests if a specific model is available by sending a chat completion request
func (s *AIAutoBanService) TestModel(baseURL, apiKey, model string) map[string]interface{} {
	config := s.getRawAIBanConfig()

	if baseURL == "" {
		baseURL, _ = config["base_url"].(string)
	}
	base := strings.TrimRight(baseURL, "/")

	if apiKey == "" {
		apiKey, _ = config["api_key"].(string)
	}
	if apiKey == "" {
		return map[string]interface{}{
			"success": false,
			"message": "API Key 未配置",
		}
	}

	testMessage := "你好，这是一条 API 连接测试消息，请简短回复确认连接正常。"

	payload := map[string]interface{}{
		"model": model,
		"messages": []map[string]string{
			{"role": "user", "content": testMessage},
		},
		"max_tokens": 100,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return map[string]interface{}{
			"success": false,
			"message": fmt.Sprintf("序列化请求失败: %s", err.Error()),
		}
	}

	url := getEndpointURL(base, "/chat/completions")
	req, err := http.NewRequest("POST", url, bytes.NewReader(payloadBytes))
	if err != nil {
		return map[string]interface{}{
			"success": false,
			"message": fmt.Sprintf("创建请求失败: %s", err.Error()),
		}
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	startTime := time.Now()
	resp, err := client.Do(req)
	elapsed := time.Since(startTime)

	if err != nil {
		msg := "连接失败，请检查 API 地址"
		if strings.Contains(err.Error(), "timeout") {
			msg = "请求超时"
		}
		return map[string]interface{}{
			"success": false,
			"message": msg,
		}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return map[string]interface{}{
			"success": false,
			"message": fmt.Sprintf("读取响应失败: %s", err.Error()),
		}
	}

	if resp.StatusCode != 200 {
		// Try to extract error detail
		var errResp struct {
			Error struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		errorDetail := string(body)
		if len(errorDetail) > 200 {
			errorDetail = errorDetail[:200]
		}
		if json.Unmarshal(body, &errResp) == nil && errResp.Error.Message != "" {
			errorDetail = errResp.Error.Message
		}
		return map[string]interface{}{
			"success": false,
			"message": fmt.Sprintf("请求失败 (%d): %s", resp.StatusCode, errorDetail),
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
		return map[string]interface{}{
			"success": false,
			"message": fmt.Sprintf("解析响应失败: %s", err.Error()),
		}
	}

	content := ""
	if len(chatResp.Choices) > 0 {
		content = chatResp.Choices[0].Message.Content
	}
	actualModel := chatResp.Model
	if actualModel == "" {
		actualModel = model
	}

	return map[string]interface{}{
		"success":      true,
		"message":      "连接成功",
		"model":        actualModel,
		"test_message": testMessage,
		"response":     content,
		"latency_ms":   elapsed.Milliseconds(),
		"usage": map[string]int{
			"prompt_tokens":     chatResp.Usage.PromptTokens,
			"completion_tokens": chatResp.Usage.CompletionTokens,
		},
	}
}

// Whitelist management

// GetWhitelist returns the whitelist user IDs
func (s *AIAutoBanService) GetWhitelist() map[string]interface{} {
	var whitelist []int64
	s.loadAIBanJSON("ai_ban:whitelist", &whitelist)

	items := make([]map[string]interface{}, 0)
	if len(whitelist) > 0 {
		// Batch query all whitelist users in one query
		placeholders := buildPlaceholders(s.db.IsPG, len(whitelist), 1)
		args := make([]interface{}, len(whitelist))
		for i, uid := range whitelist {
			args[i] = uid
		}
		query := s.db.RebindQuery(fmt.Sprintf(
			"SELECT id, username, status FROM users WHERE id IN (%s)", placeholders))
		rows, err := s.db.Query(query, args...)
		if err == nil && rows != nil {
			items = rows
		}
	}

	return map[string]interface{}{
		"items": items,
		"total": len(items),
	}
}

// AddToWhitelist adds a user to the whitelist
func (s *AIAutoBanService) AddToWhitelist(userID int64) map[string]interface{} {
	var whitelist []int64
	s.loadAIBanJSON("ai_ban:whitelist", &whitelist)

	for _, uid := range whitelist {
		if uid == userID {
			return map[string]interface{}{"message": "用户已在白名单中"}
		}
	}
	whitelist = append(whitelist, userID)
	_ = s.saveAIBanJSON("ai_ban:whitelist", whitelist)
	return map[string]interface{}{"message": fmt.Sprintf("用户 %d 已加入白名单", userID)}
}

// RemoveFromWhitelist removes a user from the whitelist
func (s *AIAutoBanService) RemoveFromWhitelist(userID int64) map[string]interface{} {
	var whitelist []int64
	s.loadAIBanJSON("ai_ban:whitelist", &whitelist)

	newList := make([]int64, 0)
	for _, uid := range whitelist {
		if uid != userID {
			newList = append(newList, uid)
		}
	}
	_ = s.saveAIBanJSON("ai_ban:whitelist", newList)
	return map[string]interface{}{"message": fmt.Sprintf("用户 %d 已从白名单移除", userID)}
}

// SearchUserForWhitelist searches users for whitelist addition
func (s *AIAutoBanService) SearchUserForWhitelist(keyword string) ([]map[string]interface{}, error) {
	// Try numeric first (user ID)
	var query string
	var args []interface{}
	if id, err := strconv.ParseInt(keyword, 10, 64); err == nil {
		query = s.db.RebindQuery(
			"SELECT id, username, status FROM users WHERE id = ? OR username LIKE ? LIMIT 20")
		args = []interface{}{id, "%" + keyword + "%"}
	} else {
		query = s.db.RebindQuery(
			"SELECT id, username, status FROM users WHERE username LIKE ? LIMIT 20")
		args = []interface{}{"%" + keyword + "%"}
	}

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}
