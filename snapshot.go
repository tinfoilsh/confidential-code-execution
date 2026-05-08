package main

// Snapshot & save a containers' filesystem.
// The bucket service handles encryption end-to-end. We pass plaintext & accessToken & encryption key
//
// Flow:
//
//   1. to resume a session: GET /items/{accessToken} from buckets with the user's
//      X-Encryption-Key, get plaintext tar back, push it into the fresh
//      container's /restore endpoint before exposing it.
//   2. to snapshot a session: ask the container for a plaintext tar (/snapshot), PUT it to
//      /items/{accessToken} with the cached exec key, then destroy the container.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// The HTTP trailer the environment sets to true once it's finished streaming
const snapshotTrailer = "X-Snapshot-Status"

// urlBase64ToStd converts the webapp's url-safe-no-padding key (idiomatic
// JS, matches the passkey/WebAuthn ecosystem) to std-base64, which is
// what buckets expects.
func urlBase64ToStd(b64url string) (string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(b64url)
	if err != nil {
		return "", fmt.Errorf("decode encryption key: %w", err)
	}
	return base64.StdEncoding.EncodeToString(raw), nil
}

// fetchSnapshotTar pulls the plaintext tar for accessToken from buckets.
// Returns (nil, nil) when the bucket has no entry for this accessToken
// (404), or when the supplied key can't open the entry (403 — wrong
// key or corrupt envelope). Both cases are non-fatal: GetOrAssign
// proceeds with a fresh empty workspace.
//
// The bearer is the user's api_key — buckets resolves it to the owning
// (user_id, org_id) and uses that as the R2 storage prefix.
func (m *Manager) fetchSnapshotTar(ctx context.Context, bearer, accessToken, codeExecutionEncryptionKeyB64 string) ([]byte, error) {
	keyStd, err := urlBase64ToStd(codeExecutionEncryptionKeyB64)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "GET", m.cfg.BucketsBase+"/items/"+accessToken, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("X-Encryption-Key", keyStd)
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
	if resp.StatusCode == http.StatusForbidden {
		// Wrong key or corrupt envelope. Caller logs and starts fresh.
		return nil, nil
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("buckets GET %s: %d %s", accessToken, resp.StatusCode, string(raw))
	}

	var body struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, fmt.Errorf("decode bucket response: %w", err)
	}
	tarBytes, err := base64.StdEncoding.DecodeString(body.Value)
	if err != nil {
		return nil, fmt.Errorf("decode bucket value: %w", err)
	}
	return tarBytes, nil
}

// putSnapshotTar PUTs the plaintext tar to buckets, encrypting under the
// supplied Code Execution Encryption Key. Buckets generates a fresh DEK
// per PUT (envelope v1) and wraps it under the supplied key.
//
// The bearer is the user's api_key — buckets resolves it to the owning
// (user_id, org_id) and uses that as the R2 storage prefix.
func (m *Manager) putSnapshotTar(ctx context.Context, bearer, accessToken, codeExecutionEncryptionKeyB64 string, tarBytes []byte) error {
	keyStd, err := urlBase64ToStd(codeExecutionEncryptionKeyB64)
	if err != nil {
		return err
	}
	body, err := json.Marshal(map[string]any{
		"value":           base64.StdEncoding.EncodeToString(tarBytes),
		"encryption_keys": []string{keyStd},
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "PUT", m.cfg.BucketsBase+"/items/"+accessToken, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "tinfoil-orchestrator/1.0")

	resp, err := m.apiClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("buckets PUT %s: %d %s", accessToken, resp.StatusCode, string(raw))
	}
	return nil
}

// pushRestore POSTs the plaintext tar to the container's /restore endpoint.
// This only succeeds during the startup window — the executor's api-server
// closes the gate after the first non-/restore call, so we MUST call this
// before any user traffic touches the container.
//
// Body shape matches executor/snapshot.go's restoreRequest: {tar: <base64>}.
// The api-server token gate also gets its first claim from this call when
// there's a snapshot to restore; on a 403 the recordContainerStatus path
// counts toward the consecutive-403s threshold like any other call.
//
// Returns (status, err). status is 0 when the request never produced an
// HTTP response (transport-level failure). The caller uses status to
// decide whether a retry is worth attempting — 4xx isn't, 5xx and 0 are.
func (m *Manager) pushRestore(c *Container, accessToken string, plaintextTar []byte) (int, error) {
	if c.httpClient == nil {
		return 0, fmt.Errorf("no http client for container %s", c.Name)
	}
	body, err := json.Marshal(map[string]string{
		"tar": base64.StdEncoding.EncodeToString(plaintextTar),
	})
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequest("POST", "https://"+c.Domain+"/restore", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Code-Execution-Access-Token", accessToken)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("restore POST: %w", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	m.recordContainerStatus(accessToken, c, resp.StatusCode)
	if resp.StatusCode >= 400 {
		return resp.StatusCode, fmt.Errorf("restore returned %d: %s", resp.StatusCode, string(data))
	}
	return resp.StatusCode, nil
}

// fetchSnapshotFromContainer asks the running container for a streaming plaintext
// tar of /workspace. Used on eviction. Encryption is handled by buckets,
// not the container, so the container response is just {tar: <base64>}.
//
// A clean stream is signaled by the X-Snapshot-Status: ok HTTP trailer.
// A 200 with the trailer absent or != "ok" means there was a problem
func (m *Manager) fetchSnapshotFromContainer(c *Container, accessToken string) ([]byte, error) {
	if c.httpClient == nil {
		return nil, fmt.Errorf("no http client for container %s", c.Name)
	}
	req, err := http.NewRequest("POST", "https://"+c.Domain+"/snapshot", bytes.NewReader([]byte("{}")))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Code-Execution-Access-Token", accessToken)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("snapshot POST: %w", err)
	}
	defer resp.Body.Close()
	data, readErr := io.ReadAll(resp.Body)
	m.recordContainerStatus(accessToken, c, resp.StatusCode)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("snapshot returned %d: %s", resp.StatusCode, string(data))
	}
	if readErr != nil {
		return nil, fmt.Errorf("read snapshot stream: %w", readErr)
	}
	// resp.Trailer is only populated after the body has been fully read.
	if got := resp.Trailer.Get(snapshotTrailer); got != "ok" {
		return nil, fmt.Errorf("snapshot stream incomplete: trailer %s=%q", snapshotTrailer, got)
	}
	var body struct {
		Tar string `json:"tar"`
	}
	if err := json.Unmarshal(data, &body); err != nil {
		return nil, fmt.Errorf("decode snapshot response: %w", err)
	}
	if body.Tar == "" {
		return nil, fmt.Errorf("snapshot returned empty tar")
	}
	tarBytes, err := base64.StdEncoding.DecodeString(body.Tar)
	if err != nil {
		return nil, fmt.Errorf("decode snapshot tar: %w", err)
	}
	return tarBytes, nil
}
