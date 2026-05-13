package main

// Coverage for extractCodeExecSecrets — the parser that pulls the three
// per-request orchestrator credentials out of params._meta.tinfoil_code_exec.
// Wire format moved off HTTP headers to avoid the middleware footgun; the
// validation rules (regex, presence, ordering) are the contract clients depend
// on, so a regression here would be silent and bad.

import (
	"strings"
	"testing"
)

const (
	// Canonical valid values used across the table.
	goodHex64    = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	goodHex64Alt = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
	// 43 base64url chars (32 bytes encoded, no padding).
	goodB64Url43 = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
)

// makeMeta builds a params map with a valid _meta.tinfoil_code_exec block,
// then applies overrides so tests can mutate individual fields succinctly.
// Use the sentinel `omit` to drop a field entirely.
type override struct {
	key   string
	value any
}

const omit = "__omit__"

func makeParams(overrides ...override) map[string]any {
	block := map[string]any{
		"accessToken":        goodHex64,
		"containerAuthToken": goodHex64Alt,
		"encryptionKey":      goodB64Url43,
	}
	for _, o := range overrides {
		if o.value == omit {
			delete(block, o.key)
		} else {
			block[o.key] = o.value
		}
	}
	return map[string]any{
		"_meta": map[string]any{
			codeExecMetaKey: block,
		},
	}
}

func TestExtractCodeExecSecrets_HappyPath(t *testing.T) {
	got, rpcErr := extractCodeExecSecrets(makeParams())
	if rpcErr != nil {
		t.Fatalf("unexpected rpcErr: %+v", rpcErr)
	}
	if got.accessToken != goodHex64 {
		t.Errorf("accessToken: got %q, want %q", got.accessToken, goodHex64)
	}
	if got.containerAuthToken != goodHex64Alt {
		t.Errorf("containerAuthToken: got %q, want %q", got.containerAuthToken, goodHex64Alt)
	}
	if got.encryptionKey != goodB64Url43 {
		t.Errorf("encryptionKey: got %q, want %q", got.encryptionKey, goodB64Url43)
	}
}

func TestExtractCodeExecSecrets_Errors(t *testing.T) {
	// Each case asserts both the rpcError.Code and a substring of the
	// message. The substrings double as the wire-format contract — they
	// appear verbatim in the README's "what does this 400 mean" guide.
	tooShortHex := strings.Repeat("a", 63)
	tooShortB64 := strings.Repeat("A", 42)
	hexWithUpper := strings.Repeat("A", 64) // hex64Re is lowercase-only

	cases := []struct {
		name        string
		params      map[string]any
		wantMessage string
	}{
		{
			name:        "missing _meta entirely",
			params:      map[string]any{},
			wantMessage: "params._meta.tinfoil_code_exec is required",
		},
		{
			name: "_meta present but tinfoil_code_exec block missing",
			params: map[string]any{
				"_meta": map[string]any{},
			},
			wantMessage: "params._meta.tinfoil_code_exec is required",
		},
		{
			name: "_meta.tinfoil_code_exec wrong type (string instead of object)",
			params: map[string]any{
				"_meta": map[string]any{codeExecMetaKey: "not-an-object"},
			},
			wantMessage: "params._meta.tinfoil_code_exec is required",
		},
		{
			name:        "accessToken missing",
			params:      makeParams(override{"accessToken", omit}),
			wantMessage: "accessToken is required",
		},
		{
			name:        "accessToken empty string",
			params:      makeParams(override{"accessToken", ""}),
			wantMessage: "accessToken is required",
		},
		{
			name:        "accessToken wrong type",
			params:      makeParams(override{"accessToken", 12345}),
			wantMessage: "accessToken is required",
		},
		{
			name:        "accessToken too short",
			params:      makeParams(override{"accessToken", tooShortHex}),
			wantMessage: "accessToken has invalid format",
		},
		{
			name:        "accessToken uppercase hex (regex is lowercase-only)",
			params:      makeParams(override{"accessToken", hexWithUpper}),
			wantMessage: "accessToken has invalid format",
		},
		{
			name:        "containerAuthToken missing",
			params:      makeParams(override{"containerAuthToken", omit}),
			wantMessage: "containerAuthToken is required",
		},
		{
			name:        "containerAuthToken bad format",
			params:      makeParams(override{"containerAuthToken", "not-hex-not-64-chars"}),
			wantMessage: "containerAuthToken has invalid format",
		},
		{
			name:        "encryptionKey missing",
			params:      makeParams(override{"encryptionKey", omit}),
			wantMessage: "encryptionKey is required",
		},
		{
			name:        "encryptionKey too short",
			params:      makeParams(override{"encryptionKey", tooShortB64}),
			wantMessage: "encryptionKey has invalid format",
		},
		{
			name:        "encryptionKey contains base64-std chars (+/=) — should reject",
			params:      makeParams(override{"encryptionKey", strings.Repeat("A", 42) + "+"}),
			wantMessage: "encryptionKey has invalid format",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, rpcErr := extractCodeExecSecrets(tc.params)
			if rpcErr == nil {
				t.Fatalf("expected rpcError, got nil")
			}
			if rpcErr.Code != -32602 {
				t.Errorf("Code: got %d, want -32602", rpcErr.Code)
			}
			if !strings.Contains(rpcErr.Message, tc.wantMessage) {
				t.Errorf("Message: got %q, want substring %q", rpcErr.Message, tc.wantMessage)
			}
		})
	}
}

// Ordering matters when multiple fields are invalid — clients debugging a
// 400 should see the *first* problem, not whichever field happens to be
// validated last. Lock the top-to-bottom check order in: accessToken →
// containerAuthToken → encryptionKey.
func TestExtractCodeExecSecrets_FailsFastInDeclaredOrder(t *testing.T) {
	all3Bad := makeParams(
		override{"accessToken", "bad-access"},
		override{"containerAuthToken", "bad-container"},
		override{"encryptionKey", "bad-enc"},
	)
	_, rpcErr := extractCodeExecSecrets(all3Bad)
	if rpcErr == nil || !strings.Contains(rpcErr.Message, "accessToken") {
		t.Fatalf("expected accessToken error first, got %+v", rpcErr)
	}

	containerAndEncBad := makeParams(
		override{"containerAuthToken", "bad-container"},
		override{"encryptionKey", "bad-enc"},
	)
	_, rpcErr = extractCodeExecSecrets(containerAndEncBad)
	if rpcErr == nil || !strings.Contains(rpcErr.Message, "containerAuthToken") {
		t.Fatalf("expected containerAuthToken error second, got %+v", rpcErr)
	}
}
