package main

// Orchestrator-side snapshot/restore helpers.
//
// The orchestrator is bytes-in/bytes-out from the controlplane's perspective:
// it never tries to verify the ciphertext, only:
//
//   1. on resume: pulls the bundle, peels off the wrappedDEK, AES-GCM-decrypts
//      the tar with the unwrapped DEK provided by the webapp, and pushes the
//      plaintext tar into the fresh container's /restore endpoint.
//   2. on eviction: asks the container for {ciphertext, wrappedDEK}, PUTs the
//      bundle to the controlplane, then destroys the container.
//
// The bundle wire format is the executor's snapshotResponse JSON verbatim:
//
//   { "ciphertext": "<base64-std>", "wrappedDEK": "<base64-std>" }
//
// where ciphertext is `nonce(12) || ct||tag` of the tar under the DEK
// (matches executor/snapshot.go aesGCMEncrypt). The orchestrator stores it
// as a single record so ciphertext and wrappedDEK can't get out of sync.

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// Per-request session attributes are passed down the call chain through
// context. The MCP boundary reads X-Exec-Pubkey / X-Exec-Resume-Dek from
// request headers and stashes them here; GetOrAssign reads them back at
// the assign point. Lifetime is bounded by the request goroutine — when
// the handler returns, the context is gone, so a stale DEK can't leak
// into a later request.
type ctxKey int

const (
	ctxKeyPubkey ctxKey = iota
	ctxKeyResumeDEK
	ctxKeyBearer
)

// WithSessionAttrs returns a child context carrying the user pubkey,
// optional resume DEK, and the user's bearer token. Empty values are
// not stored, so callers downstream (GetOrAssign, restoreInto) see
// them as absent. The bearer is used for the snapshot GET on the
// restore-on-assign path — controlplane requires a fresh user JWT
// for reads, which we have during a live tool/call request.
func WithSessionAttrs(ctx context.Context, pubkey, resumeDEK, bearer string) context.Context {
	if pubkey != "" {
		ctx = context.WithValue(ctx, ctxKeyPubkey, pubkey)
	}
	if resumeDEK != "" {
		ctx = context.WithValue(ctx, ctxKeyResumeDEK, resumeDEK)
	}
	if bearer != "" {
		ctx = context.WithValue(ctx, ctxKeyBearer, bearer)
	}
	return ctx
}

func sessionPubkey(ctx context.Context) string {
	v, _ := ctx.Value(ctxKeyPubkey).(string)
	return v
}

func sessionResumeDEK(ctx context.Context) string {
	v, _ := ctx.Value(ctxKeyResumeDEK).(string)
	return v
}

func sessionBearer(ctx context.Context) string {
	v, _ := ctx.Value(ctxKeyBearer).(string)
	return v
}

// snapshotBundle is what we PUT to and GET from the controlplane.
// Same shape as the executor's snapshotResponse.
type snapshotBundle struct {
	Ciphertext string `json:"ciphertext"`
	WrappedDEK string `json:"wrappedDEK"`
}

// decryptSnapshotTar reverses executor's aesGCMEncrypt: format is
// nonce(12) || ciphertext_with_tag. dek must be 32 bytes (AES-256-GCM).
func decryptSnapshotTar(dek, ciphertext []byte) ([]byte, error) {
	if len(dek) != 32 {
		return nil, fmt.Errorf("dek must be 32 bytes, got %d", len(dek))
	}
	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(ciphertext) < gcm.NonceSize()+gcm.Overhead() {
		return nil, fmt.Errorf("ciphertext too short: %d", len(ciphertext))
	}
	nonce := ciphertext[:gcm.NonceSize()]
	ct := ciphertext[gcm.NonceSize():]
	plain, err := gcm.Open(nil, nonce, ct, nil)
	if err != nil {
		return nil, fmt.Errorf("aes-gcm open: %w", err)
	}
	return plain, nil
}

// onBehalfOfHeader formats the X-On-Behalf-Of header set on
// admin-authed snapshot PUTs. Empty when no Clerk user is bound (which
// shouldn't happen in production: AuthorizeSession rejects non-Clerk
// traffic before a container is assigned. Empty here means the call
// is from a path that never went through MCP — most likely a test).
func onBehalfOfHeader(clerkUserID string) map[string]string {
	if clerkUserID == "" {
		return nil
	}
	return map[string]string{"X-On-Behalf-Of": clerkUserID}
}

// fetchSnapshotBundle pulls the full {ciphertext, wrappedDEK} bundle for
// the given execSessionId from controlplane. Returns (nil, nil) if no
// bundle exists (404).
//
// Uses the user's bearer JWT — controlplane scopes GETs by the JWT
// subject and rejects admin-authed reads. This works because GETs only
// run during a live tool/call request, when the user's JWT is fresh.
// Empty bearer means we're in a test path that never went through MCP;
// we still issue the request so existing tests against fake
// controlplanes (which don't enforce auth) keep working.
func (m *Manager) fetchSnapshotBundle(ctx context.Context, execSessionID, bearer string) (*snapshotBundle, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", apiBase+"/api/storage/exec-snapshot/"+execSessionID, nil)
	if err != nil {
		return nil, err
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	req.Header.Set("User-Agent", "tinfoil-orchestrator/1.0")

	resp, err := m.apiClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("controlplane GET snapshot %s: %d %s", execSessionID, resp.StatusCode, string(raw))
	}
	var b snapshotBundle
	if err := json.Unmarshal(raw, &b); err != nil {
		return nil, fmt.Errorf("decode bundle: %w", err)
	}
	return &b, nil
}

// putSnapshotBundle uploads {ciphertext, wrappedDEK} to controlplane,
// attributing the row to clerkUserID via X-On-Behalf-Of. Admin-authed:
// PUTs run from the eviction loop, long after the user's JWT expired.
func (m *Manager) putSnapshotBundle(execSessionID, clerkUserID string, b *snapshotBundle) error {
	status, raw, err := m.apiRequestWithHeaders("PUT", "/api/storage/exec-snapshot/"+execSessionID, b, onBehalfOfHeader(clerkUserID))
	if err != nil {
		return err
	}
	if status >= 400 {
		return fmt.Errorf("controlplane PUT snapshot %s: %d %s", execSessionID, status, string(raw))
	}
	return nil
}

// pushRestore POSTs the plaintext tar to the container's /restore endpoint.
// This only succeeds during the startup window — the executor's api-server
// closes the gate after the first non-/restore call, so we MUST call this
// before any user traffic touches the container.
//
// Body shape matches executor/snapshot.go's restoreRequest: {tar: <base64>}.
func (m *Manager) pushRestore(c *Container, plaintextTar []byte) error {
	if c.httpClient == nil {
		return fmt.Errorf("no http client for container %s", c.Name)
	}
	body, err := json.Marshal(map[string]string{
		"tar": base64.StdEncoding.EncodeToString(plaintextTar),
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", "https://"+c.Domain+"/restore", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("restore POST: %w", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("restore returned %d: %s", resp.StatusCode, string(data))
	}
	return nil
}

// fetchSnapshotFromContainer asks the running container for a snapshot
// bundle, wrapped to the given user pubkey. Used on eviction.
func (m *Manager) fetchSnapshotFromContainer(c *Container, userPubkeyB64 string) (*snapshotBundle, error) {
	if c.httpClient == nil {
		return nil, fmt.Errorf("no http client for container %s", c.Name)
	}
	body, err := json.Marshal(map[string]string{"pubkey": userPubkeyB64})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", "https://"+c.Domain+"/snapshot", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("snapshot POST: %w", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("snapshot returned %d: %s", resp.StatusCode, string(data))
	}
	var b snapshotBundle
	if err := json.Unmarshal(data, &b); err != nil {
		return nil, fmt.Errorf("decode snapshot response: %w", err)
	}
	if b.Ciphertext == "" || b.WrappedDEK == "" {
		return nil, fmt.Errorf("snapshot returned empty fields")
	}
	return &b, nil
}
