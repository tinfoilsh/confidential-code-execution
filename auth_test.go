package main

// Tests for the orchestrator's session→user binding. The whoami round
// trip is faked with a stub controlplane so we can exercise the
// caching + mismatch logic without a live Clerk verifier.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// fakeWhoami stands in for controlplane's GET /api/auth/whoami. Maps
// known bearer tokens to Clerk user IDs; anything else 401s. Counts
// calls so tests can assert that the orchestrator caches by
// bearer-hash instead of re-validating per call.
type fakeWhoami struct {
	*httptest.Server
	calls atomic.Int64
}

func newFakeWhoami(users map[string]string) *fakeWhoami {
	f := &fakeWhoami{}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/auth/whoami", func(w http.ResponseWriter, r *http.Request) {
		f.calls.Add(1)
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") {
			http.Error(w, "missing bearer", http.StatusUnauthorized)
			return
		}
		token := strings.TrimPrefix(auth, "Bearer ")
		userID, ok := users[token]
		if !ok {
			http.Error(w, "invalid token", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"clerk_user_id": userID})
	})
	f.Server = httptest.NewServer(mux)
	return f
}

func newAuthTestManager(t *testing.T, cpURL string) *Manager {
	t.Helper()
	prev := apiBase
	apiBase = cpURL
	t.Cleanup(func() { apiBase = prev })
	return NewManager(ManagerConfig{AdminAPIKey: "x", IdleTimeout: time.Hour})
}

func TestAuthorizeSession_EmptyBearer(t *testing.T) {
	cp := newFakeWhoami(nil)
	defer cp.Close()
	m := newAuthTestManager(t, cp.URL)

	if _, err := m.AuthorizeSession(context.Background(), "sess", ""); !errors.Is(err, ErrAuthRequired) {
		t.Fatalf("want ErrAuthRequired, got %v", err)
	}
	if got := cp.calls.Load(); got != 0 {
		t.Fatalf("empty bearer should short-circuit before whoami; got %d calls", got)
	}
}

func TestAuthorizeSession_ControlplaneRejects(t *testing.T) {
	cp := newFakeWhoami(map[string]string{}) // every token 401s
	defer cp.Close()
	m := newAuthTestManager(t, cp.URL)

	if _, err := m.AuthorizeSession(context.Background(), "sess", "not-a-jwt"); !errors.Is(err, ErrAuthRequired) {
		t.Fatalf("want ErrAuthRequired on 401 from controlplane, got %v", err)
	}
}

func TestAuthorizeSession_BindsAndCaches(t *testing.T) {
	cp := newFakeWhoami(map[string]string{"jwt-A": "user_alice"})
	defer cp.Close()
	m := newAuthTestManager(t, cp.URL)

	uid, err := m.AuthorizeSession(context.Background(), "sess", "jwt-A")
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	if uid != "user_alice" {
		t.Fatalf("user mismatch: %q", uid)
	}
	if got := cp.calls.Load(); got != 1 {
		t.Fatalf("first call should hit whoami once, got %d", got)
	}

	// Same bearer, same session → cache hit, no extra whoami.
	uid2, err := m.AuthorizeSession(context.Background(), "sess", "jwt-A")
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if uid2 != "user_alice" {
		t.Fatalf("second call user mismatch: %q", uid2)
	}
	if got := cp.calls.Load(); got != 1 {
		t.Fatalf("repeat call must not re-hit whoami; got %d total calls", got)
	}

	m.mu.Lock()
	bound := m.identities["sess"]
	m.mu.Unlock()
	if bound == nil || bound.clerkUserID != "user_alice" {
		t.Fatalf("identities[sess] = %+v, want clerkUserID=user_alice", bound)
	}
}

func TestAuthorizeSession_TokenRefreshSameUser(t *testing.T) {
	// Alice's session lives across a token refresh — both old and new
	// JWTs map to user_alice. Re-validation fires (different hash) but
	// identity matches, so no error and the cached hash advances.
	cp := newFakeWhoami(map[string]string{
		"jwt-A1": "user_alice",
		"jwt-A2": "user_alice",
	})
	defer cp.Close()
	m := newAuthTestManager(t, cp.URL)

	if _, err := m.AuthorizeSession(context.Background(), "sess", "jwt-A1"); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if _, err := m.AuthorizeSession(context.Background(), "sess", "jwt-A2"); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if got := cp.calls.Load(); got != 2 {
		t.Fatalf("expected 2 whoami calls (bind + refresh), got %d", got)
	}

	// Now jwt-A2 should be cached — third call short-circuits.
	if _, err := m.AuthorizeSession(context.Background(), "sess", "jwt-A2"); err != nil {
		t.Fatalf("post-refresh repeat: %v", err)
	}
	if got := cp.calls.Load(); got != 2 {
		t.Fatalf("post-refresh repeat must not re-hit whoami; got %d", got)
	}
}

func TestAuthorizeSession_IdentityMismatch(t *testing.T) {
	// Alice binds the session; Bob tries to reuse it with his own
	// (validly verified) JWT. Must 403, not silently succeed.
	cp := newFakeWhoami(map[string]string{
		"jwt-A": "user_alice",
		"jwt-B": "user_bob",
	})
	defer cp.Close()
	m := newAuthTestManager(t, cp.URL)

	if _, err := m.AuthorizeSession(context.Background(), "sess", "jwt-A"); err != nil {
		t.Fatalf("alice bind: %v", err)
	}
	if _, err := m.AuthorizeSession(context.Background(), "sess", "jwt-B"); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("want ErrIdentityMismatch, got %v", err)
	}

	// Alice can still use her session.
	if uid, err := m.AuthorizeSession(context.Background(), "sess", "jwt-A"); err != nil || uid != "user_alice" {
		t.Fatalf("alice still bound? uid=%q err=%v", uid, err)
	}
}

func TestAuthorizeSession_DifferentSessionsIndependent(t *testing.T) {
	// Alice and Bob each have their own session — no interference.
	cp := newFakeWhoami(map[string]string{
		"jwt-A": "user_alice",
		"jwt-B": "user_bob",
	})
	defer cp.Close()
	m := newAuthTestManager(t, cp.URL)

	if uid, err := m.AuthorizeSession(context.Background(), "sess-A", "jwt-A"); err != nil || uid != "user_alice" {
		t.Fatalf("alice: uid=%q err=%v", uid, err)
	}
	if uid, err := m.AuthorizeSession(context.Background(), "sess-B", "jwt-B"); err != nil || uid != "user_bob" {
		t.Fatalf("bob: uid=%q err=%v", uid, err)
	}
}

func TestExtractBearer(t *testing.T) {
	cases := map[string]string{
		"":                "",
		"Bearer xyz":      "xyz",
		"Bearer ":         "",
		"bearer xyz":      "", // case-sensitive on purpose; we only accept canonical form
		"Token xyz":       "",
		"Bearer xyz abc":  "xyz abc",
	}
	for in, want := range cases {
		if got := extractBearer(in); got != want {
			t.Errorf("extractBearer(%q) = %q, want %q", in, got, want)
		}
	}
}
