package main

// Tests for the orchestrator's Clerk JWT gate. The validate-jwt round
// trip is faked with a stub controlplane so we can exercise auth
// without a live Clerk verifier.

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// newFakeValidateJWT stands in for controlplane's GET
// /api/auth/validate-jwt. Returns 200 for any bearer in `valid`,
// 401 otherwise.
func newFakeValidateJWT(valid map[string]bool) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/auth/validate-jwt", func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") {
			http.Error(w, "missing bearer", http.StatusUnauthorized)
			return
		}
		token := strings.TrimPrefix(auth, "Bearer ")
		if !valid[token] {
			http.Error(w, "invalid token", http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	return httptest.NewServer(mux)
}

func newAuthTestManager(t *testing.T, cpURL string) *Manager {
	t.Helper()
	prev := apiBase
	apiBase = cpURL
	t.Cleanup(func() { apiBase = prev })
	return NewManager(ManagerConfig{AdminAPIKey: "x", IdleTimeout: time.Hour})
}

func TestAuthorizeSession_EmptyBearer(t *testing.T) {
	cp := newFakeValidateJWT(nil)
	defer cp.Close()
	m := newAuthTestManager(t, cp.URL)

	if err := m.AuthorizeSession(context.Background(), ""); !errors.Is(err, ErrAuthRequired) {
		t.Fatalf("want ErrAuthRequired, got %v", err)
	}
}

func TestAuthorizeSession_ControlplaneRejects(t *testing.T) {
	cp := newFakeValidateJWT(map[string]bool{}) // every token 401s
	defer cp.Close()
	m := newAuthTestManager(t, cp.URL)

	if err := m.AuthorizeSession(context.Background(), "not-a-jwt"); !errors.Is(err, ErrAuthRequired) {
		t.Fatalf("want ErrAuthRequired on 401 from controlplane, got %v", err)
	}
}

func TestAuthorizeSession_ValidBearer(t *testing.T) {
	cp := newFakeValidateJWT(map[string]bool{"jwt-A": true})
	defer cp.Close()
	m := newAuthTestManager(t, cp.URL)

	if err := m.AuthorizeSession(context.Background(), "jwt-A"); err != nil {
		t.Fatalf("valid bearer should pass, got %v", err)
	}
}

func TestExtractBearer(t *testing.T) {
	cases := map[string]string{
		"":               "",
		"Bearer xyz":     "xyz",
		"Bearer ":        "",
		"bearer xyz":     "", // case-sensitive on purpose; we only accept canonical form
		"Token xyz":      "",
		"Bearer xyz abc": "xyz abc",
	}
	for in, want := range cases {
		if got := extractBearer(in); got != want {
			t.Errorf("extractBearer(%q) = %q, want %q", in, got, want)
		}
	}
}
