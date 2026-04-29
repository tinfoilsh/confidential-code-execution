package main

// Code execution is webapp-only for v1: every session must bind to a
// verified Clerk user. The orchestrator delegates JWT verification to
// the controlplane's /api/auth/whoami endpoint to avoid embedding the
// Clerk SDK across services. Once verified, the (sessionID → user)
// binding lives on Manager.identities; subsequent calls with a
// different verified identity are rejected, and snapshots are
// attributed to the bound user via X-On-Behalf-Of on the
// controlplane PUT.

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// sessionIdentity records the Clerk user a session is bound to, plus a
// hash of the bearer last verified for it. We only re-call whoami when
// the hash changes (token refresh) or on the first call in a session,
// so steady-state per-tool-call cost is a map lookup, not a network
// hop.
type sessionIdentity struct {
	clerkUserID string
	bearerHash  [32]byte
}

// ErrAuthRequired is returned when the request lacks a Clerk JWT.
// Bare API keys can't open a code-execution session in v1.
var ErrAuthRequired = errors.New("code execution requires a Clerk-authenticated session")

// ErrIdentityMismatch is returned when a request to an existing
// session arrives with a different verified user than the one that
// originally bound it. Catches the cross-user proxy attack.
var ErrIdentityMismatch = errors.New("session is bound to a different user")

// AuthorizeSession verifies the request's bearer via controlplane
// whoami, binds the session on first call, and rejects mismatched
// identity on subsequent calls. Returns the bound Clerk user ID.
func (m *Manager) AuthorizeSession(ctx context.Context, sessionID, bearer string) (string, error) {
	if bearer == "" {
		return "", ErrAuthRequired
	}
	hash := sha256.Sum256([]byte(bearer))

	m.mu.Lock()
	bound, ok := m.identities[sessionID]
	m.mu.Unlock()
	if ok && bound.bearerHash == hash {
		return bound.clerkUserID, nil
	}

	userID, err := m.whoami(ctx, bearer)
	if err != nil {
		return "", err
	}
	if userID == "" {
		return "", ErrAuthRequired
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if bound, ok := m.identities[sessionID]; ok {
		if bound.clerkUserID != userID {
			return "", ErrIdentityMismatch
		}
		// Same user, fresh token: refresh the hash so the next call
		// short-circuits without another whoami round-trip.
		bound.bearerHash = hash
		return userID, nil
	}
	m.identities[sessionID] = &sessionIdentity{
		clerkUserID: userID,
		bearerHash:  hash,
	}
	return userID, nil
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

// SessionIdentity returns the bound Clerk user ID for a session, or
// "" if none is bound. Used at restore/eviction time so the snapshot
// PUT/GET can carry X-On-Behalf-Of.
func (m *Manager) SessionIdentity(sessionID string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if id, ok := m.identities[sessionID]; ok {
		return id.clerkUserID
	}
	return ""
}

// extractBearer pulls the token out of an Authorization header, or
// returns "" if the header isn't a Bearer scheme.
func extractBearer(authHeader string) string {
	if !strings.HasPrefix(authHeader, "Bearer ") {
		return ""
	}
	return strings.TrimPrefix(authHeader, "Bearer ")
}
