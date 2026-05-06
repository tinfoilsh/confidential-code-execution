package main

// Every code-exec request carries an api_key as the bearer — webapp mints
// a chat key (tk_*) at sign-in and uses it for inference and code-exec
// alike, and external callers hit the router with their own api_keys.
//
// The orchestrator delegates identity resolution to controlplane's
// POST /api/shim/identity, which looks the api_key up in the api_keys
// table and returns 200 if it exists. We use it as a yes/no gate — we
// don't care about the identity itself; downstream services (buckets)
// re-resolve when they need to scope writes.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// ErrAuthRequired is returned when the request lacks a recognized api_key
// (no `Authorization`, controlplane returns 401).
var ErrAuthRequired = errors.New("code execution requires an authenticated session")

// AuthorizeSession verifies the bearer is a known api_key via controlplane
// /api/shim/identity. No caching, no binding — single round trip per tool
// call.
//
// Short-circuited to nil when cfg.SkipJWTValidation is true
// (SKIP_JWT_VALIDATION=true). Local dev only.
func (m *Manager) AuthorizeSession(ctx context.Context, bearer string) error {
	if m.cfg.SkipJWTValidation {
		return nil
	}
	if bearer == "" {
		return ErrAuthRequired
	}
	return m.resolveIdentity(ctx, bearer)
}

// resolveIdentity asks controlplane to recognize the api_key. Identity
// payload is discarded — we only care that the lookup succeeded.
//
//   - nil                on a recognized api_key (200)
//   - ErrAuthRequired    when controlplane returns 401 / 403
//   - other err          on transport / unexpected status
func (m *Manager) resolveIdentity(ctx context.Context, bearer string) error {
	body, err := json.Marshal(map[string]string{"token": bearer})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", apiBase+"/api/shim/identity", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "tinfoil-orchestrator/1.0")

	resp, err := m.apiClient.Do(req)
	if err != nil {
		return fmt.Errorf("controlplane shim/identity: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	case http.StatusUnauthorized, http.StatusForbidden:
		return ErrAuthRequired
	default:
		return fmt.Errorf("controlplane shim/identity: status %d", resp.StatusCode)
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
