package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

var ErrAuthRequired = errors.New("code execution requires a valid api key")

// AuthorizeSession verifies the bearer is a valid api_key by calling
// controlplane's /api/shim/validate-key.
//
//   - nil                   on a valid api_key (200)
//   - ErrAuthRequired       when bearer is empty or controlplane refuses
//   - other err             on transport / unexpected upstream status
func (m *Manager) AuthorizeSession(ctx context.Context, bearer string) error {
	if bearer == "" {
		return ErrAuthRequired
	}
	body, err := json.Marshal(map[string]string{"api_key": bearer})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", apiBase+"/api/shim/validate-key", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "tinfoil-orchestrator/1.0")

	resp, err := m.apiClient.Do(req)
	if err != nil {
		return fmt.Errorf("controlplane validate-key: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	// common controlplane errors - 401, 402, 403, 429
	case http.StatusUnauthorized,
		http.StatusPaymentRequired,
		http.StatusForbidden,
		http.StatusTooManyRequests:
		return ErrAuthRequired
	default:
		return fmt.Errorf("controlplane validate-key: status %d", resp.StatusCode)
	}
}

func extractBearer(authHeader string) string {
	if !strings.HasPrefix(authHeader, "Bearer ") {
		return ""
	}
	return strings.TrimPrefix(authHeader, "Bearer ")
}
