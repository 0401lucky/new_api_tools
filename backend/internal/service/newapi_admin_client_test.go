package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNewAPIAdminClientBlackroomExternalBan(t *testing.T) {
	var got blackroomExternalBanPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/blackroom/external-ban" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "admin-token" {
			t.Fatalf("unexpected Authorization header: %q", r.Header.Get("Authorization"))
		}
		if r.Header.Get("New-Api-User") != "42" {
			t.Fatalf("unexpected New-Api-User header: %q", r.Header.Get("New-Api-User"))
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"success":true}`))
	}))
	defer server.Close()

	client := &newapiAdminClient{
		baseURL:     server.URL + "/",
		apiKey:      "admin-token",
		adminUserID: 42,
		httpClient:  server.Client(),
	}

	err := client.BlackroomExternalBan(123, 13, "外部风控", `{"risk":true}`, false)
	if err != nil {
		t.Fatalf("external ban should succeed: %v", err)
	}
	if got.UserID != 123 || got.IpCount != 13 || got.Reason != "外部风控" || got.Evidence != `{"risk":true}` || got.Permanent {
		t.Fatalf("unexpected payload: %+v", got)
	}
}

func TestNewAPIAdminClientBlackroomExternalBanError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"success":false,"message":"拒绝封禁"}`))
	}))
	defer server.Close()

	client := &newapiAdminClient{
		baseURL:     server.URL,
		apiKey:      "admin-token",
		adminUserID: 42,
		httpClient:  server.Client(),
	}

	if err := client.BlackroomExternalBan(404, 0, "", "", true); err == nil {
		t.Fatal("expected new-api error")
	}
}
