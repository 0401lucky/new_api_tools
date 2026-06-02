package service

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/new-api-tools/backend/internal/config"
)

type newapiAdminClient struct {
	baseURL     string
	apiKey      string
	adminUserID int64
	httpClient  *http.Client
}

type blackroomExternalBanPayload struct {
	UserID    int64  `json:"user_id"`
	IpCount   int64  `json:"ip_count"`
	Reason    string `json:"reason"`
	Evidence  string `json:"evidence"`
	Permanent bool   `json:"permanent"`
}

type newapiResponseEnvelope struct {
	Success bool            `json:"success"`
	Message string          `json:"message"`
	Error   json.RawMessage `json:"error"`
}

func newNewAPIAdminClient() *newapiAdminClient {
	cfg := config.Get()
	return &newapiAdminClient{
		baseURL:     cfg.NewAPIBaseURL,
		apiKey:      cfg.NewAPIKey,
		adminUserID: cfg.NewAPIAdminUserID,
		httpClient:  &http.Client{Timeout: 15 * time.Second},
	}
}

func validateNewAPIExternalBanConfig() error {
	cfg := config.Get()
	if strings.TrimSpace(cfg.NewAPIBaseURL) == "" {
		return errors.New("NEWAPI_BASEURL 未配置")
	}
	if strings.TrimSpace(cfg.NewAPIKey) == "" {
		return errors.New("NEWAPI_API_KEY 未配置")
	}
	if cfg.NewAPIAdminUserID <= 0 {
		return errors.New("NEWAPI_ADMIN_USER_ID 未配置")
	}
	return nil
}

func (c *newapiAdminClient) doPost(endpoint string, payload interface{}) error {
	if c == nil {
		return errors.New("new-api client is nil")
	}
	baseURL := strings.TrimRight(strings.TrimSpace(c.baseURL), "/")
	if baseURL == "" {
		return errors.New("NEWAPI_BASEURL 未配置")
	}
	apiKey := strings.TrimSpace(c.apiKey)
	if apiKey == "" {
		return errors.New("NEWAPI_API_KEY 未配置")
	}
	if c.adminUserID <= 0 {
		return errors.New("NEWAPI_ADMIN_USER_ID 未配置")
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, baseURL+endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", apiKey)
	req.Header.Set("New-Api-User", fmt.Sprintf("%d", c.adminUserID))
	req.Header.Set("Content-Type", "application/json")

	httpClient := c.httpClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("new-api 返回 HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	var envelope newapiResponseEnvelope
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &envelope); err != nil {
			return err
		}
	}
	if !envelope.Success {
		if envelope.Message != "" {
			return errors.New(envelope.Message)
		}
		if len(envelope.Error) > 0 && string(envelope.Error) != "null" {
			return fmt.Errorf("new-api 返回失败: %s", string(envelope.Error))
		}
		return errors.New("new-api 返回失败")
	}
	return nil
}

// BlackroomExternalBan 调用 new-api 将用户纳入小黑屋（来源 external）。
func (c *newapiAdminClient) BlackroomExternalBan(userID, ipCount int64, reason, evidence string, permanent bool) error {
	return c.doPost("/api/blackroom/external-ban", blackroomExternalBanPayload{
		UserID:    userID,
		IpCount:   ipCount,
		Reason:    reason,
		Evidence:  evidence,
		Permanent: permanent,
	})
}
