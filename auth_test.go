package main

// Tests for the orchestrator's identity gate. The /api/shim/identity
// round trip is faked with a stub controlplane so we can exercise auth
// without a live api_keys DB.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// newFakeIdentity stands in for controlplane's POST /api/shim/identity.
// 200 for any token in `valid`, 401 otherwise.
func newFakeIdentity(valid map[string]bool) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/shim/identity", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			APIKey string `json:"api_key"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		if !valid[body.APIKey] {
			http.Error(w, "invalid api key", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"user_id": "user_x"})
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
	cp := newFakeIdentity(nil)
	defer cp.Close()
	m := newAuthTestManager(t, cp.URL)

	if err := m.AuthorizeSession(context.Background(), ""); !errors.Is(err, ErrAuthRequired) {
		t.Fatalf("want ErrAuthRequired, got %v", err)
	}
}

func TestAuthorizeSession_ControlplaneRejects(t *testing.T) {
	cp := newFakeIdentity(map[string]bool{}) // every token 401s
	defer cp.Close()
	m := newAuthTestManager(t, cp.URL)

	if err := m.AuthorizeSession(context.Background(), "tk_unknown"); !errors.Is(err, ErrAuthRequired) {
		t.Fatalf("want ErrAuthRequired on 401 from controlplane, got %v", err)
	}
}

func TestAuthorizeSession_ValidBearer(t *testing.T) {
	cp := newFakeIdentity(map[string]bool{"tk_known": true})
	defer cp.Close()
	m := newAuthTestManager(t, cp.URL)

	if err := m.AuthorizeSession(context.Background(), "tk_known"); err != nil {
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
