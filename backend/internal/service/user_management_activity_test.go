package service

import (
	"reflect"
	"testing"
	"time"
)

func installUserManagementSchema(t *testing.T) {
	t.Helper()
	db := installSQLiteForTests(t)
	stmts := []string{
		`CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			username TEXT,
			display_name TEXT,
			email TEXT,
			role INTEGER,
			status INTEGER,
			quota INTEGER,
			used_quota INTEGER,
			request_count INTEGER,
			"group" TEXT,
			aff_code TEXT,
			remark TEXT,
			deleted_at TEXT
		)`,
		`CREATE TABLE logs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			user_id INTEGER,
			created_at INTEGER,
			type INTEGER
		)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("create schema: %v", err)
		}
	}

	now := time.Now().Unix()
	users := []struct {
		id           int
		username     string
		requestCount int
		deletedAt    interface{}
	}{
		{1, "active-user", 3, nil},
		{2, "inactive-user", 2, nil},
		{3, "very-inactive-user", 5, nil},
		{4, "never-user", 0, nil},
		{5, "no-log-user", 1, nil},
		{6, "deleted-user", 4, "2026-01-01"},
	}
	for _, u := range users {
		_, err := db.Exec(
			`INSERT INTO users (id, username, display_name, email, role, status, quota, used_quota, request_count, "group", aff_code, remark, deleted_at)
			 VALUES (?, ?, '', '', 1, 1, 0, 0, ?, 'default', '', '', ?)`,
			u.id, u.username, u.requestCount, u.deletedAt,
		)
		if err != nil {
			t.Fatalf("insert user %s: %v", u.username, err)
		}
	}

	logs := []struct {
		userID    int
		createdAt int64
	}{
		{1, now - 24*3600},
		{2, now - 10*24*3600},
		{3, now - 40*24*3600},
		{6, now - 24*3600},
	}
	for _, l := range logs {
		if _, err := db.Exec(`INSERT INTO logs (user_id, created_at, type) VALUES (?, ?, 2)`, l.userID, l.createdAt); err != nil {
			t.Fatalf("insert log for user %d: %v", l.userID, err)
		}
	}
}

func TestGetUsersFiltersByActivity(t *testing.T) {
	installUserManagementSchema(t)
	svc := NewUserManagementService()

	tests := []struct {
		name      string
		filter    string
		wantTotal int64
		wantUsers []string
	}{
		{"active", ActivityActive, 1, []string{"active-user"}},
		{"inactive", ActivityInactive, 1, []string{"inactive-user"}},
		{"very inactive", ActivityVeryInactive, 2, []string{"very-inactive-user", "no-log-user"}},
		{"never", ActivityNever, 1, []string{"never-user"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := svc.GetUsers(ListUsersParams{
				Page:           1,
				PageSize:       100,
				ActivityFilter: tt.filter,
				OrderBy:        "id",
				OrderDir:       "ASC",
			})
			if err != nil {
				t.Fatalf("GetUsers failed: %v", err)
			}

			if got := toInt64(result["total"]); got != tt.wantTotal {
				t.Fatalf("expected total %d, got %d", tt.wantTotal, got)
			}

			items, ok := result["items"].([]map[string]interface{})
			if !ok {
				t.Fatalf("items has unexpected type %T", result["items"])
			}

			gotUsers := make([]string, 0, len(items))
			for _, item := range items {
				gotUsers = append(gotUsers, toString(item["username"]))
				if got := toString(item["activity_level"]); got != tt.filter {
					t.Fatalf("user %s expected activity %s, got %s", item["username"], tt.filter, got)
				}
			}
			if !reflect.DeepEqual(gotUsers, tt.wantUsers) {
				t.Fatalf("expected users %v, got %v", tt.wantUsers, gotUsers)
			}
		})
	}
}
