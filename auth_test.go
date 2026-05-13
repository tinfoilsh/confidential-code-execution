//go:build !dev

package main

// Tests for the orchestrator's api_key gate. The /api/shim/validate-key
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

// newFakeValidateKey stands in for controlplane's POST /api/shim/validate-key.
// 200 for any token in `valid`, otherwise the configured rejectStatus
// (defaults to 401 — unknown key).
func newFakeValidateKey(valid map[string]bool, rejectStatus int) *httptest.Server {
	if rejectStatus == 0 {
		rejectStatus = http.StatusUnauthorized
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/shim/validate-key", func(w http.ResponseWriter, r *http.Request) {
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
			http.Error(w, "rejected", rejectStatus)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	return httptest.NewServer(mux)
}

func newAuthTestManager(t *testing.T, cpURL string) *Manager {
	t.Helper()
	return NewManager(ManagerConfig{
		ScopedCodeExecAdminKey:     "x",
		ControlPlaneURL: cpURL,
		IdleTimeout:     time.Hour,
	})
}

func TestAuthorizeSession_EmptyBearer(t *testing.T) {
	cp := newFakeValidateKey(nil, 0)
	defer cp.Close()
	m := newAuthTestManager(t, cp.URL)

	if err := m.AuthorizeSession(context.Background(), ""); !errors.Is(err, ErrAuthRequired) {
		t.Fatalf("want ErrAuthRequired, got %v", err)
	}
}

func TestAuthorizeSession_ControlplaneRejects(t *testing.T) {
	cp := newFakeValidateKey(map[string]bool{}, http.StatusUnauthorized)
	defer cp.Close()
	m := newAuthTestManager(t, cp.URL)

	if err := m.AuthorizeSession(context.Background(), "tk_unknown"); !errors.Is(err, ErrAuthRequired) {
		t.Fatalf("want ErrAuthRequired on 401 from controlplane, got %v", err)
	}
}

func TestAuthorizeSession_BillingDisabled(t *testing.T) {
	// 402 Payment Required: key exists but is disabled for billing
	// reasons. Treated as "refuse the request" the same as an unknown key.
	cp := newFakeValidateKey(map[string]bool{}, http.StatusPaymentRequired)
	defer cp.Close()
	m := newAuthTestManager(t, cp.URL)

	if err := m.AuthorizeSession(context.Background(), "tk_billing_off"); !errors.Is(err, ErrAuthRequired) {
		t.Fatalf("want ErrAuthRequired on 402 from controlplane, got %v", err)
	}
}

func TestAuthorizeSession_RateLimited(t *testing.T) {
	cp := newFakeValidateKey(map[string]bool{}, http.StatusTooManyRequests)
	defer cp.Close()
	m := newAuthTestManager(t, cp.URL)

	if err := m.AuthorizeSession(context.Background(), "tk_rl"); !errors.Is(err, ErrAuthRequired) {
		t.Fatalf("want ErrAuthRequired on 429 from controlplane, got %v", err)
	}
}

func TestAuthorizeSession_ValidBearer(t *testing.T) {
	cp := newFakeValidateKey(map[string]bool{"tk_known": true}, 0)
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
