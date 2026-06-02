package service

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/new-api-tools/backend/internal/cache"
	"github.com/new-api-tools/backend/internal/config"
)

func clearAIBanTestCache(t *testing.T) {
	t.Helper()
	cache.Get().DeleteByPrefix("ai_ban:")
}

func installAIBanSchema(t *testing.T) {
	t.Helper()
	db := installSQLiteForTests(t)
	stmts := []string{
		`CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			username TEXT,
			display_name TEXT,
			email TEXT,
			status INTEGER,
			"group" TEXT,
			remark TEXT,
			linux_do_id TEXT,
			request_count INTEGER,
			role INTEGER,
			deleted_at TEXT
		)`,
		`CREATE TABLE logs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			user_id INTEGER,
			username TEXT,
			created_at INTEGER,
			type INTEGER,
			model_name TEXT,
			quota INTEGER,
			prompt_tokens INTEGER,
			completion_tokens INTEGER,
			use_time REAL,
			ip TEXT,
			channel_id INTEGER,
			channel_name TEXT,
			token_id INTEGER,
			token_name TEXT,
			"group" TEXT
		)`,
		`CREATE TABLE tokens (
			id INTEGER PRIMARY KEY,
			user_id INTEGER,
			status INTEGER
		)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("create schema: %v", err)
		}
	}
}

func seedAIBanUser(t *testing.T, role int) {
	t.Helper()
	db := NewAIAutoBanService().db.DB
	now := time.Now().Unix()
	if _, err := db.Exec(`INSERT INTO users (id, username, display_name, status, "group", role, request_count)
		VALUES (1, 'alice', 'Alice', 1, 'default', ?, 100)`, role); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO tokens (id, user_id, status) VALUES (10, 1, 1)`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 12; i++ {
		logType := 2
		completion := 100
		if i%3 == 0 {
			logType = 5
			completion = 0
		}
		if _, err := db.Exec(`INSERT INTO logs (
			user_id, username, created_at, type, model_name, quota, prompt_tokens,
			completion_tokens, use_time, ip, channel_id, channel_name, token_id, token_name, "group"
		) VALUES (1, 'alice', ?, ?, 'gpt-test', 100, 50, ?, 1.2, '203.0.113.8', 1, 'openai', 10, 'main', 'default')`,
			now-int64(i*30), logType, completion); err != nil {
			t.Fatal(err)
		}
	}
}

func seedAIBanUserWithOldLogs(t *testing.T) {
	t.Helper()
	db := NewAIAutoBanService().db.DB
	if _, err := db.Exec(`INSERT INTO users (id, username, display_name, status, "group", role, request_count)
		VALUES (1, 'alice', 'Alice', 1, 'default', 1, 100)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO tokens (id, user_id, status) VALUES (10, 1, 1)`); err != nil {
		t.Fatal(err)
	}

	// 保证日志超过 1 小时但仍在默认的 24 小时扫描窗口内。
	baseTime := time.Now().Unix() - 2*3600
	for i := 0; i < 12; i++ {
		if _, err := db.Exec(`INSERT INTO logs (
			user_id, username, created_at, type, model_name, quota, prompt_tokens,
			completion_tokens, use_time, ip, channel_id, channel_name, token_id, token_name, "group"
		) VALUES (1, 'alice', ?, 5, 'gpt-test', 100, 50, 0, 1.2, '203.0.113.8', 1, 'openai', 10, 'main', 'default')`,
			baseTime+int64(i*10)); err != nil {
			t.Fatal(err)
		}
	}
}

func seedAIBanSwitchingUser(t *testing.T) {
	t.Helper()
	db := NewAIAutoBanService().db.DB
	now := time.Now().Unix()
	if _, err := db.Exec(`INSERT INTO users (id, username, display_name, status, "group", role, request_count)
		VALUES (2, 'switcher', 'Switcher', 1, 'default', 1, 100)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO tokens (id, user_id, status) VALUES (20, 2, 1)`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 12; i++ {
		ip := fmt.Sprintf("198.51.100.%d", (i%4)+1)
		if _, err := db.Exec(`INSERT INTO logs (
			user_id, username, created_at, type, model_name, quota, prompt_tokens,
			completion_tokens, use_time, ip, channel_id, channel_name, token_id, token_name, "group"
		) VALUES (2, 'switcher', ?, 2, 'gpt-test', 100, 50, 100, 1.2, ?, 1, 'openai', 20, 'main', 'default')`,
			now-int64((12-i)*10), ip); err != nil {
			t.Fatal(err)
		}
	}
}

func seedAIBanManyIPUser(t *testing.T, userID int64, uniqueIPs, totalRequests int) {
	t.Helper()
	db := NewAIAutoBanService().db.DB
	now := time.Now().Unix()
	username := fmt.Sprintf("manyip%d", userID)
	tokenID := userID * 10
	if _, err := db.Exec(`INSERT INTO users (id, username, display_name, status, "group", role, request_count)
		VALUES (?, ?, ?, 1, 'default', 1, ?)`, userID, username, username, totalRequests); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO tokens (id, user_id, status) VALUES (?, ?, 1)`, tokenID, userID); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < totalRequests; i++ {
		ip := fmt.Sprintf("198.51.100.%d", (i%uniqueIPs)+1)
		if _, err := db.Exec(`INSERT INTO logs (
			user_id, username, created_at, type, model_name, quota, prompt_tokens,
			completion_tokens, use_time, ip, channel_id, channel_name, token_id, token_name, "group"
		) VALUES (?, ?, ?, 2, 'gpt-test', 100, 50, 100, 1.2, ?, 1, 'openai', ?, 'main', 'default')`,
			userID, username, now-int64(totalRequests-i), ip, tokenID); err != nil {
			t.Fatal(err)
		}
	}
}

func mockAIBanServer(t *testing.T, content string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Fatalf("missing bearer token")
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"model": "gpt-test",
			"choices": []map[string]interface{}{
				{"message": map[string]string{"content": content}},
			},
			"usage": map[string]int{
				"prompt_tokens":     123,
				"completion_tokens": 45,
			},
		})
	}))
}

func saveAIBanTestConfig(t *testing.T, baseURL string, dryRun bool) {
	t.Helper()
	if err := NewAIAutoBanService().SaveConfig(map[string]interface{}{
		"base_url":              baseURL,
		"api_key":               "test-key",
		"model":                 "gpt-test",
		"enabled":               true,
		"dry_run":               dryRun,
		"scan_interval_minutes": 0,
	}); err != nil {
		t.Fatal(err)
	}
}

func hasAIBanFlag(row map[string]interface{}, flag string) bool {
	for _, item := range toStringSlice(row["risk_flags"]) {
		if item == flag {
			return true
		}
	}
	return false
}

func TestAIBanConfigMasksAPIKey(t *testing.T) {
	clearAIBanTestCache(t)
	installAIBanSchema(t)
	saveAIBanTestConfig(t, "https://api.example.test", true)

	config := NewAIAutoBanService().GetConfig()
	if _, ok := config["api_key"]; ok {
		t.Fatalf("public config should not expose api_key: %#v", config)
	}
	if config["has_api_key"] != true {
		t.Fatalf("public config should expose has_api_key=true: %#v", config)
	}
	if toString(config["masked_api_key"]) == "" || strings.Contains(toString(config["masked_api_key"]), "test-key") {
		t.Fatalf("public config should expose only masked key: %#v", config)
	}
}

func TestParseAIBanAssessmentJSONVariants(t *testing.T) {
	plain, err := parseAIBanAssessment(`{"should_ban":true,"risk_score":9,"confidence":0.8,"action":"ban","reason":"高风险"}`)
	if err != nil {
		t.Fatalf("plain json parse failed: %v", err)
	}
	if !plain.ShouldBan || plain.RiskScore != 9 || plain.Confidence != 0.8 {
		t.Fatalf("unexpected plain assessment: %+v", plain)
	}

	fenced, err := parseAIBanAssessment("```json\n{\"should_ban\":false,\"risk_score\":3,\"confidence\":70,\"action\":\"monitor\",\"reason\":\"观察\"}\n```")
	if err != nil {
		t.Fatalf("fenced json parse failed: %v", err)
	}
	if fenced.ShouldBan || fenced.Confidence != 0.7 || fenced.Action != "monitor" {
		t.Fatalf("unexpected fenced assessment: %+v", fenced)
	}

	if _, err := parseAIBanAssessment("不是 JSON"); err == nil {
		t.Fatal("invalid json should fail")
	}
}

func TestBuildAIBanPromptReplacesCustomVariables(t *testing.T) {
	config := map[string]interface{}{
		"custom_prompt":   "用户 {user_id}/{username} 请求 {total_requests} 次，风险 {risk_flags}，黑名单命中 {user_blacklisted_ips}，系统黑名单 {blacklist_ips}",
		"blacklist_ips":   []string{"203.0.113.8"},
		"whitelist_ips":   []string{"198.51.100.1"},
		"excluded_models": []string{},
		"excluded_groups": []string{},
	}
	analysis := map[string]interface{}{
		"user": map[string]interface{}{
			"id":           42,
			"username":     "alice",
			"display_name": "Alice",
			"group":        "default",
		},
		"summary": map[string]interface{}{
			"total_requests": 12,
			"unique_models":  1,
			"unique_tokens":  1,
			"unique_ips":     1,
		},
		"risk": map[string]interface{}{
			"risk_flags": []string{"BLACKLIST_IP"},
			"ip_switch_analysis": map[string]interface{}{
				"switch_count":        0,
				"rapid_switch_count":  0,
				"avg_ip_duration":     0,
				"min_switch_interval": 0,
			},
		},
		"top_ips": []map[string]interface{}{{"ip": "203.0.113.8"}},
	}
	prompt, err := (&AIAutoBanService{}).buildAIBanPrompt(config, analysis, map[string]interface{}{
		"whitelist_ips": []string{},
		"blacklist_ips": []string{"203.0.113.8"},
	}, map[string]interface{}{})
	if err != nil {
		t.Fatalf("build prompt failed: %v", err)
	}
	for _, placeholder := range []string{"{user_id}", "{total_requests}", "{user_blacklisted_ips}", "{blacklist_ips}"} {
		if strings.Contains(prompt, placeholder) {
			t.Fatalf("prompt should replace %s: %s", placeholder, prompt)
		}
	}
	for _, expected := range []string{"42/Alice", "12 次", "BLACKLIST_IP", "203.0.113.8"} {
		if !strings.Contains(prompt, expected) {
			t.Fatalf("prompt missing %q: %s", expected, prompt)
		}
	}
}

func TestParseAIBanAssessmentRepairsCommonModelJSON(t *testing.T) {
	content := `{
		should_ban: false
		risk_score: 2,
		confidence: 85,
		action: normal,
		reason: "单IP单Token稳定调用"
	}`
	got, err := parseAIBanAssessment(content)
	if err != nil {
		t.Fatalf("repair parse failed: %v", err)
	}
	if got.ShouldBan || got.RiskScore != 2 || got.Confidence != 0.85 || got.Action != "normal" {
		t.Fatalf("unexpected repaired assessment: %+v", got)
	}
}

func TestCallAIBanModelFallsBackWhenJSONModeUnsupported(t *testing.T) {
	clearAIBanTestCache(t)
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		body, _ := io.ReadAll(r.Body)
		if calls == 1 {
			if !strings.Contains(string(body), "response_format") {
				t.Fatalf("first call should request json mode: %s", string(body))
			}
			http.Error(w, `{"error":{"message":"response_format json_object is unsupported"}}`, http.StatusBadRequest)
			return
		}
		if strings.Contains(string(body), "response_format") {
			t.Fatalf("fallback call should omit response_format: %s", string(body))
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"model": "gpt-test",
			"choices": []map[string]interface{}{
				{"message": map[string]string{"content": `{"should_ban":false,"risk_score":2,"confidence":0.9,"action":"normal","reason":"正常"}`}},
			},
		})
	}))
	defer server.Close()

	got, err := (&AIAutoBanService{}).callAIBanModel(map[string]interface{}{
		"base_url": server.URL,
		"api_key":  "test-key",
		"model":    "gpt-test",
	}, "审查")
	if err != nil {
		t.Fatalf("call should fallback: %v", err)
	}
	if calls != 2 || got.Action != "normal" {
		t.Fatalf("unexpected fallback result calls=%d got=%+v", calls, got)
	}
}

func TestCallAIBanModelRetriesAfterParseFailure(t *testing.T) {
	clearAIBanTestCache(t)
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		content := "我认为这个用户正常，但我不输出 JSON"
		if calls == 2 {
			content = `{"should_ban":false,"risk_score":3,"confidence":0.8,"action":"normal","reason":"重试后正常"}`
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"model": "gpt-test",
			"choices": []map[string]interface{}{
				{"message": map[string]string{"content": content}},
			},
			"usage": map[string]int{
				"prompt_tokens":     10,
				"completion_tokens": 5,
			},
		})
	}))
	defer server.Close()

	got, err := (&AIAutoBanService{}).callAIBanModel(map[string]interface{}{
		"base_url": server.URL,
		"api_key":  "test-key",
		"model":    "gpt-test",
	}, "审查")
	if err != nil {
		t.Fatalf("call should retry parse failure: %v", err)
	}
	if calls != 2 || got.Reason != "重试后正常" || got.PromptTokens != 10 {
		t.Fatalf("unexpected retry result calls=%d got=%+v", calls, got)
	}
}

func TestCallAIBanModelParsesArrayContent(t *testing.T) {
	clearAIBanTestCache(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"model": "gemini-test",
			"choices": []map[string]interface{}{
				{
					"message": map[string]interface{}{
						"content": []map[string]string{
							{
								"type": "text",
								"text": `{"should_ban":false,"risk_score":4,"confidence":0.8,"action":"monitor","reason":"数组内容块"}`,
							},
						},
					},
				},
			},
			"usage": map[string]int{
				"prompt_tokens":     11,
				"completion_tokens": 7,
			},
		})
	}))
	defer server.Close()

	got, err := (&AIAutoBanService{}).callAIBanModel(map[string]interface{}{
		"base_url": server.URL,
		"api_key":  "test-key",
		"model":    "gemini-test",
	}, "审查")
	if err != nil {
		t.Fatalf("array content should parse: %v", err)
	}
	if got.Action != "monitor" || got.Reason != "数组内容块" || got.PromptTokens != 11 {
		t.Fatalf("unexpected array content assessment: %+v", got)
	}
}

func TestCallAIBanModelParsesReasoningContentWhenContentEmpty(t *testing.T) {
	clearAIBanTestCache(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"model": "gemini-test",
			"choices": []map[string]interface{}{
				{
					"message": map[string]interface{}{
						"content":           "",
						"reasoning_content": `{"should_ban":true,"risk_score":8,"confidence":0.82,"action":"ban","reason":"reasoning 字段 JSON"}`,
					},
				},
			},
		})
	}))
	defer server.Close()

	got, err := (&AIAutoBanService{}).callAIBanModel(map[string]interface{}{
		"base_url": server.URL,
		"api_key":  "test-key",
		"model":    "gemini-test",
	}, "审查")
	if err != nil {
		t.Fatalf("reasoning content should parse: %v", err)
	}
	if !got.ShouldBan || got.Action != "ban" || got.Reason != "reasoning 字段 JSON" {
		t.Fatalf("unexpected reasoning content assessment: %+v", got)
	}
}

func TestAIBanThresholdNormalizationIsConservative(t *testing.T) {
	low := aiBanAssessment{ShouldBan: true, RiskScore: 7, Confidence: 0.9, Action: "ban", Reason: "分数不足"}
	normalizeAIBanAssessment(&low)
	if low.ShouldBan || low.Action == "ban" {
		t.Fatalf("low score should not auto-ban: %+v", low)
	}

	high := aiBanAssessment{ShouldBan: true, RiskScore: 8, Confidence: 0.75, Action: "ban", Reason: "高风险"}
	normalizeAIBanAssessment(&high)
	if !high.ShouldBan || decideAIBanAction(high) != "ban" {
		t.Fatalf("high confidence should auto-ban: %+v", high)
	}
}

func TestAIBanIPVolumeClassificationBoundaries(t *testing.T) {
	if flag, _ := classifyAIBanIPVolume(8, 30); flag != "" {
		t.Fatalf("8 IPs should not be upgraded by IP volume alone, got %s", flag)
	}
	if flag, _ := classifyAIBanIPVolume(9, 30); flag != "MANY_IPS" {
		t.Fatalf("9 IPs should warn, got %s", flag)
	}
	if flag, _ := classifyAIBanIPVolume(15, 30); flag != "MANY_IPS_SEVERE" {
		t.Fatalf("15 IPs should be severe, got %s", flag)
	}
	if flag, _ := classifyAIBanIPVolume(20, 30); flag != "MANY_IPS_EXTREME" {
		t.Fatalf("20 IPs should be extreme, got %s", flag)
	}
	if flag, _ := classifyAIBanIPVolume(15, 19); flag != "MANY_IPS" {
		t.Fatalf("15 IPs with too few requests should warn, got %s", flag)
	}
}

func TestAIBanAPIHealthSuspendsAndResets(t *testing.T) {
	clearAIBanTestCache(t)
	svc := &AIAutoBanService{}
	svc.recordAIAPIFailure("one")
	svc.recordAIAPIFailure("two")
	state := svc.recordAIAPIFailure("three")
	if state.SuspendedUntil <= time.Now().Unix() {
		t.Fatalf("expected suspended health state: %+v", state)
	}
	health := svc.getAPIHealthMap()
	if health["suspended"] != true {
		t.Fatalf("expected suspended health map: %#v", health)
	}
	svc.resetAPIHealth()
	health = svc.getAPIHealthMap()
	if health["suspended"] == true || toInt64(health["consecutive_failures"]) != 0 {
		t.Fatalf("expected reset health: %#v", health)
	}
}

func TestAIBanAuditLogsKeepLatestThousand(t *testing.T) {
	clearAIBanTestCache(t)
	svc := &AIAutoBanService{}
	for i := 0; i < aiBanAuditLogLimit+1; i++ {
		svc.appendAuditLog(map[string]interface{}{"scan_id": i})
	}
	var logs []map[string]interface{}
	cache.Get().GetJSON(aiBanAuditLogsKey, &logs)
	if len(logs) != aiBanAuditLogLimit {
		t.Fatalf("expected %d logs, got %d", aiBanAuditLogLimit, len(logs))
	}
	if toInt64(logs[0]["scan_id"]) != aiBanAuditLogLimit {
		t.Fatalf("latest log should be kept first, got %#v", logs[0])
	}
}

func TestAIBanSuspiciousUsersPrioritizesIPSwitching(t *testing.T) {
	clearAIBanTestCache(t)
	installAIBanSchema(t)
	seedAIBanUser(t, 1)
	seedAIBanSwitchingUser(t)

	rows, err := NewAIAutoBanService().GetSuspiciousUsers("1h", 2)
	if err != nil {
		t.Fatalf("get suspicious users failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected two candidates, got %#v", rows)
	}
	if toInt64(rows[0]["user_id"]) != 2 {
		t.Fatalf("ip switching user should rank first: %#v", rows)
	}
	if !hasAIBanFlag(rows[0], "IP_RAPID_SWITCH") || !hasAIBanFlag(rows[0], "IP_HOPPING") {
		t.Fatalf("expected IP switching flags, got %#v", rows[0])
	}
	if toInt64(rows[0]["rapid_switch_count"]) == 0 {
		t.Fatalf("rapid_switch_count should be populated: %#v", rows[0])
	}
}

func TestAIBanSuspiciousUsersFlagsSevereManyIPs(t *testing.T) {
	clearAIBanTestCache(t)
	installAIBanSchema(t)
	seedAIBanManyIPUser(t, 3, 15, 30)

	rows, err := NewAIAutoBanService().GetSuspiciousUsers("24h", 1)
	if err != nil {
		t.Fatalf("get suspicious users failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected one candidate, got %#v", rows)
	}
	if !hasAIBanFlag(rows[0], "MANY_IPS_SEVERE") {
		t.Fatalf("expected severe many IP flag, got %#v", rows[0])
	}
	if score := toFloat64(rows[0]["suspicion_score"]); score < 55 {
		t.Fatalf("severe many IP score too low: %#v", rows[0])
	}
}

func TestAIBanManualAssessOverridesNormalModelForSevereManyIPs(t *testing.T) {
	clearAIBanTestCache(t)
	installAIBanSchema(t)
	seedAIBanManyIPUser(t, 3, 15, 30)
	server := mockAIBanServer(t, `{"should_ban":false,"risk_score":2,"confidence":0.85,"action":"normal","reason":"Cloudflare 节点轮转，正常"}`)
	defer server.Close()
	saveAIBanTestConfig(t, server.URL, true)

	result := NewAIAutoBanService().ManualAssess(3, "24h")
	if result["action"] != "ban" {
		t.Fatalf("severe many IPs should override normal model result: %#v", result)
	}
	assessment := result["assessment"].(map[string]interface{})
	if assessment["should_ban"] != true || assessment["model_should_ban"] != false {
		t.Fatalf("override should preserve model decision and force local ban: %#v", assessment)
	}
	if toFloat64(assessment["risk_score"]) < 8 || toFloat64(assessment["confidence"]) < 0.8 {
		t.Fatalf("override should raise score and confidence: %#v", assessment)
	}
	override := result["rule_override"].(map[string]interface{})
	if override["rule"] != "MANY_IPS_SEVERE" || toInt64(override["unique_ips"]) != 15 {
		t.Fatalf("override details should describe severe many IPs: %#v", override)
	}
}

func TestAIBanManualAssessFallsBackToLocalRulesWhenModelOutputInvalid(t *testing.T) {
	clearAIBanTestCache(t)
	installAIBanSchema(t)
	seedAIBanManyIPUser(t, 3, 15, 30)
	server := mockAIBanServer(t, "Gemini returned tokens, but no JSON object")
	defer server.Close()
	saveAIBanTestConfig(t, server.URL, true)

	result := NewAIAutoBanService().ManualAssess(3, "24h")
	if result["action"] != "ban" {
		t.Fatalf("invalid model output should still use local severe many IP rule: %#v", result)
	}
	if toString(result["ai_error"]) == "" {
		t.Fatalf("result should keep AI parse error for diagnostics: %#v", result)
	}
	assessment := result["assessment"].(map[string]interface{})
	if assessment["should_ban"] != true || toFloat64(assessment["risk_score"]) < 8 {
		t.Fatalf("fallback assessment should be ban-ready: %#v", assessment)
	}
	if !strings.Contains(toString(assessment["raw_response"]), "Gemini returned tokens") {
		t.Fatalf("fallback assessment should preserve raw response: %#v", assessment)
	}
}

func TestAIBanRunScanDryRunDoesNotMutateUser(t *testing.T) {
	clearAIBanTestCache(t)
	installAIBanSchema(t)
	seedAIBanUser(t, 1)
	server := mockAIBanServer(t, `{"should_ban":true,"risk_score":9,"confidence":0.9,"action":"ban","reason":"高失败率和异常请求"}`)
	defer server.Close()
	saveAIBanTestConfig(t, server.URL, true)

	result := NewAIAutoBanService().RunScan("1h", 10)
	stats := result["stats"].(map[string]interface{})
	if toInt64(stats["banned_count"]) != 1 {
		t.Fatalf("dry run should record one ban candidate, got %#v", stats)
	}

	row, err := NewAIAutoBanService().db.QueryOne(`SELECT status FROM users WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if toInt64(row["status"]) != 1 {
		t.Fatalf("dry run should not ban user, status=%v", row["status"])
	}
	token, err := NewAIAutoBanService().db.QueryOne(`SELECT status FROM tokens WHERE id = 10`)
	if err != nil {
		t.Fatal(err)
	}
	if toInt64(token["status"]) != 1 {
		t.Fatalf("dry run should not disable token, status=%v", token["status"])
	}
}

func TestAIBanRunScheduledScanUsesDefaultWindowForLegacyConfig(t *testing.T) {
	clearAIBanTestCache(t)
	installAIBanSchema(t)
	seedAIBanUserWithOldLogs(t)
	server := mockAIBanServer(t, `{"should_ban":true,"risk_score":9,"confidence":0.9,"action":"ban","reason":"24 小时窗口内高失败率"}`)
	defer server.Close()

	// 直接写入旧配置形态，刻意不包含 scan_window，验证调度扫描会补默认值。
	if err := cache.Get().Set(aiBanConfigKey, map[string]interface{}{
		"base_url":              server.URL,
		"api_key":               "test-key",
		"model":                 "gpt-test",
		"enabled":               true,
		"dry_run":               true,
		"scan_interval_minutes": 1,
	}, 0); err != nil {
		t.Fatal(err)
	}

	result := NewAIAutoBanService().RunScheduledScan()
	stats, ok := result["stats"].(map[string]interface{})
	if !ok {
		t.Fatalf("scheduled scan should run, got %#v", result)
	}
	if stats["window"] != "24h" {
		t.Fatalf("legacy config should use default 24h window, got %#v", stats)
	}
	if toInt64(stats["banned_count"]) != 1 {
		t.Fatalf("24h scheduled scan should find old suspicious user, got %#v", stats)
	}
}

func TestAIBanRunScanFormalSubmitsBlackroomExternalBan(t *testing.T) {
	clearAIBanTestCache(t)
	installAIBanSchema(t)
	seedAIBanUser(t, 1)
	server := mockAIBanServer(t, `{"should_ban":true,"risk_score":9,"confidence":0.9,"action":"ban","reason":"高风险滥用"}`)
	defer server.Close()
	saveAIBanTestConfig(t, server.URL, false)

	var got blackroomExternalBanPayload
	newAPIServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/blackroom/external-ban" {
			t.Fatalf("unexpected new-api path: %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "admin-token" {
			t.Fatalf("unexpected Authorization header: %q", r.Header.Get("Authorization"))
		}
		if r.Header.Get("New-Api-User") != "42" {
			t.Fatalf("unexpected New-Api-User header: %q", r.Header.Get("New-Api-User"))
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode external ban payload: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"success":true}`))
	}))
	defer newAPIServer.Close()
	t.Setenv("NEWAPI_BASEURL", newAPIServer.URL)
	t.Setenv("NEWAPI_API_KEY", "admin-token")
	t.Setenv("NEWAPI_ADMIN_USER_ID", "42")
	config.Load()

	result := NewAIAutoBanService().RunScan("1h", 10)
	stats := result["stats"].(map[string]interface{})
	if toInt64(stats["banned_count"]) != 1 {
		t.Fatalf("formal scan should record one ban, got %#v", stats)
	}
	if got.UserID != 1 || got.Reason != "高风险滥用" || got.Permanent {
		t.Fatalf("formal scan should submit external ban payload, got %+v", got)
	}
	if got.IpCount <= 0 {
		t.Fatalf("formal scan should pass ip_count to new-api, got %+v", got)
	}
	if !strings.Contains(got.Evidence, `"source":"external_ai_ban"`) {
		t.Fatalf("formal scan should include evidence, got %s", got.Evidence)
	}

	row, err := NewAIAutoBanService().db.QueryOne(`SELECT status FROM users WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if toInt64(row["status"]) != 1 {
		t.Fatalf("formal scan should not directly update local user status, status=%v", row["status"])
	}
	token, err := NewAIAutoBanService().db.QueryOne(`SELECT status FROM tokens WHERE id = 10`)
	if err != nil {
		t.Fatal(err)
	}
	if toInt64(token["status"]) != 1 {
		t.Fatalf("formal scan should not directly disable token, status=%v", token["status"])
	}
}

func TestAIBanManualAssessProtectsAdmin(t *testing.T) {
	clearAIBanTestCache(t)
	installAIBanSchema(t)
	seedAIBanUser(t, 10)
	server := mockAIBanServer(t, `{"should_ban":true,"risk_score":10,"confidence":1,"action":"ban","reason":"不应调用"}`)
	defer server.Close()
	saveAIBanTestConfig(t, server.URL, false)

	result := NewAIAutoBanService().ManualAssess(1, "1h")
	if result["protected"] != true || result["skipped"] != true {
		t.Fatalf("admin should be protected: %#v", result)
	}
	row, err := NewAIAutoBanService().db.QueryOne(`SELECT status FROM users WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if toInt64(row["status"]) != 1 {
		t.Fatalf("protected admin should remain active, status=%v", row["status"])
	}
}
