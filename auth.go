package main

// Code execution is webapp-only for v1: every tools/call must carry a
// real Clerk JWT (we don't check who it is, just that it exists).
// The orchestrator delegates JWT verification to the controlplane's
// /api/auth/validate-jwt endpoint to avoid embedding the Clerk SDK
// across services.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// ErrAuthRequired is returned when the request lacks a valid Clerk JWT.
var ErrAuthRequired = errors.New("code execution requires a Clerk-authenticated session")

// AuthorizeSession verifies the request's bearer is a real Clerk JWT
// via controlplane /api/auth/validate-jwt. No caching, no binding —
// just a single round trip per tool call.
func (m *Manager) AuthorizeSession(ctx context.Context, bearer string) error {
	if bearer == "" {
		return ErrAuthRequired
	}
	return m.validateJWT(ctx, bearer)
}

// validateJWT asks controlplane to confirm the bearer is a valid Clerk JWT.
// Returns:
//   - nil                on a verified Clerk JWT (200)
//   - ErrAuthRequired    when controlplane returns 401 (not a Clerk JWT)
//   - other err          on transport / unexpected status
func (m *Manager) validateJWT(ctx context.Context, bearer string) error {
	req, err := http.NewRequestWithContext(ctx, "GET", apiBase+"/api/auth/validate-jwt", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("User-Agent", "tinfoil-orchestrator/1.0")

	resp, err := m.apiClient.Do(req)
	if err != nil {
		return fmt.Errorf("controlplane validate-jwt: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	case http.StatusUnauthorized, http.StatusForbidden:
		return ErrAuthRequired
	default:
		return fmt.Errorf("controlplane validate-jwt: status %d", resp.StatusCode)
	}
}

// extractBearer pulls the token out of an Authorization header, or
// returns "" if the header isn't a Bearer scheme.
func extractBearer(authHeader string) string {
	if !strings.HasPrefix(authHeader, "Bearer ") {
		return ""
	}
	return strings.TrimPrefix(authHeader, "Bearer ")
}
