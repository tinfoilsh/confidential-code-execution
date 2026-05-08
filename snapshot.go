package main

// Snapshot & restore a container's filesystem.
// The bucket service handles encryption end-to-end. We pass plaintext, accessToken, encryption key, api key
//
//   1. resume: GET /items/{accessToken} from buckets, push plaintext
//      tar to the fresh container's /restore before user traffic hits it.
//   2. snapshot: ask the container for a plaintext tar at /snapshot,
//      PUT to /items/{accessToken}, then destroy the container.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// HTTP trailer the environment sets to "ok" once it's finished streaming.
const snapshotTrailer = "X-Snapshot-Status"

// ---------------------------------------------------------------------------
// Buckets — HTTP client for buckets.tinfoil.sh.
// ---------------------------------------------------------------------------

type Buckets struct {
	baseURL    string
	httpClient *http.Client
}

func NewBuckets(baseURL string) *Buckets {
	return &Buckets{
		baseURL:    baseURL,
		httpClient: &http.Client{Timeout: 120 * time.Second}, //
	}
}

// urlBase64ToStd converts the webapp's url-safe-no-padding key
// (idiomatic JS) to std-base64 — what buckets expects.
func urlBase64ToStd(b64url string) (string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(b64url)
	if err != nil {
		return "", fmt.Errorf("decode encryption key: %w", err)
	}
	return base64.StdEncoding.EncodeToString(raw), nil
}

// fetch returns the plaintext tar for accessToken. (nil, nil) on 403/404
// — wrong key or no snapshot yet, both surface as a fresh workspace.
func (b *Buckets) fetch(ctx context.Context, bearer, accessToken, codeExecutionEncryptionKeyB64 string) ([]byte, error) {
	keyStd, err := urlBase64ToStd(codeExecutionEncryptionKeyB64)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "GET", b.baseURL+"/items/"+accessToken, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("X-Encryption-Key", keyStd)
	req.Header.Set("User-Agent", "tinfoil-orchestrator/1.0")

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusForbidden {
		return nil, nil
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("buckets GET: %d", resp.StatusCode)
	}
	raw, err := readLimited(resp.Body, maxSnapshotBody)
	if err != nil {
		return nil, fmt.Errorf("buckets GET body: %w", err)
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

// put stores the plaintext tar; buckets encrypts it under the supplied key.
func (b *Buckets) put(ctx context.Context, bearer, accessToken, codeExecutionEncryptionKeyB64 string, tarBytes []byte) error {
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
	req, err := http.NewRequestWithContext(ctx, "PUT", b.baseURL+"/items/"+accessToken, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "tinfoil-orchestrator/1.0")

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("buckets PUT: %d", resp.StatusCode)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Executor /restore + /snapshot — hits the per-container attested client.
// ---------------------------------------------------------------------------

// pushRestore POSTs the plaintext tar to the container's /restore.
// Must run before any user traffic touches the container. A 403 counts
// toward the consecutive-403s threshold via recordContainerStatus.
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
	_, _ = readLimited(resp.Body, maxExecutorBody)
	m.recordContainerStatus(accessToken, c, resp.StatusCode)
	if resp.StatusCode >= 400 {
		return resp.StatusCode, fmt.Errorf("restore returned %d", resp.StatusCode)
	}
	return resp.StatusCode, nil
}

// Streams a plaintext tar of /workspace.
// Clean stream is signaled by the X-Snapshot-Status: ok HTTP trailer;
// 200 with the trailer absent or != "ok" means there was a problem.
func (m *Manager) fetchSnapshotFromContainer(ctx context.Context, c *Container, accessToken string) ([]byte, error) {
	if c.httpClient == nil {
		return nil, fmt.Errorf("no http client for container %s", c.Name)
	}
	req, err := http.NewRequestWithContext(ctx, "POST", "https://"+c.Domain+"/snapshot", bytes.NewReader([]byte("{}")))
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
	data, readErr := readLimited(resp.Body, maxSnapshotBody)
	m.recordContainerStatus(accessToken, c, resp.StatusCode)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("snapshot returned %d", resp.StatusCode)
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
