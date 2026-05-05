package main

// Code execution is webapp-only for v1: every tools/call must carry a
// real Clerk JWT (we don't check who it is, just that it exists).
// The orchestrator delegates JWT verification to the
// controlplane's /api/auth/whoami endpoint to avoid embedding the
// Clerk SDK across services.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// ErrAuthRequired is returned when the request lacks a valid Clerk JWT.
var ErrAuthRequired = errors.New("code execution requires a Clerk-authenticated session")

// AuthorizeSession verifies the request's bearer is a real Clerk JWT
// via controlplane whoami. No caching, no binding — just a single
// round trip per tool call.
func (m *Manager) AuthorizeSession(ctx context.Context, bearer string) error {
	if bearer == "" {
		return ErrAuthRequired
	}
	userID, err := m.whoami(ctx, bearer)
	if err != nil {
		return err
	}
	if userID == "" {
		return ErrAuthRequired
	}
	return nil
}

// whoami asks controlplane to resolve the user behind a bearer.
// Returns:
//   - clerk_user_id, nil  on a verified Clerk JWT
//   - "", nil             when controlplane returns 401 (not a Clerk JWT)
//   - "", err             on transport / unexpected status
func (m *Manager) whoami(ctx context.Context, bearer string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", apiBase+"/api/auth/whoami", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("User-Agent", "tinfoil-orchestrator/1.0")

	resp, err := m.apiClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("controlplane whoami: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return "", nil
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("controlplane whoami: status %d", resp.StatusCode)
	}

	var body struct {
		ClerkUserID string `json:"clerk_user_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", fmt.Errorf("decode whoami: %w", err)
	}
	return body.ClerkUserID, nil
}

// extractBearer pulls the token out of an Authorization header, or
// returns "" if the header isn't a Bearer scheme.
func extractBearer(authHeader string) string {
	if !strings.HasPrefix(authHeader, "Bearer ") {
		return ""
	}
	return strings.TrimPrefix(authHeader, "Bearer ")
}
