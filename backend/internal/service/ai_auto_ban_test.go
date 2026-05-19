package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/new-api-tools/backend/internal/cache"
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

func TestAIBanRunScanFormalBansUserAndToken(t *testing.T) {
	clearAIBanTestCache(t)
	installAIBanSchema(t)
	seedAIBanUser(t, 1)
	server := mockAIBanServer(t, `{"should_ban":true,"risk_score":9,"confidence":0.9,"action":"ban","reason":"高风险滥用"}`)
	defer server.Close()
	saveAIBanTestConfig(t, server.URL, false)

	result := NewAIAutoBanService().RunScan("1h", 10)
	stats := result["stats"].(map[string]interface{})
	if toInt64(stats["banned_count"]) != 1 {
		t.Fatalf("formal scan should record one ban, got %#v", stats)
	}

	row, err := NewAIAutoBanService().db.QueryOne(`SELECT status FROM users WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if toInt64(row["status"]) != 2 {
		t.Fatalf("formal scan should ban user, status=%v", row["status"])
	}
	token, err := NewAIAutoBanService().db.QueryOne(`SELECT status FROM tokens WHERE id = 10`)
	if err != nil {
		t.Fatal(err)
	}
	if toInt64(token["status"]) != 2 {
		t.Fatalf("formal scan should disable token, status=%v", token["status"])
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
