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

// 1 initial + 2 retries on transient (5xx or network) failures.
const (
	bucketFetchAttempts = 3
	bucketPutAttempts   = 3
	pushRestoreAttempts = 3
)

// Backoff between retries. Vars (not consts) so tests can zero them.
var (
	snapshotPutRetryDelay = 1 * time.Second
	restorePushRetryDelay = 1 * time.Second
)

// httpRetry calls fn up to `attempts` times, sleeping `backoff` between
// calls. fn returns (result, transient, err); transient=true means the
// error is worth retrying.
func httpRetry[T any](ctx context.Context, attempts int, backoff time.Duration, fn func() (T, bool, error)) (T, error) {
	var zero T
	var lastErr error
	for i := 0; i < attempts; i++ {
		if i > 0 {
			select {
			case <-ctx.Done():
				return zero, ctx.Err()
			case <-time.After(backoff):
			}
		}
		result, transient, err := fn()
		if err == nil {
			return result, nil
		}
		lastErr = err
		if !transient {
			return zero, err
		}
	}
	return zero, lastErr
}

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
		httpClient: &http.Client{Timeout: 120 * time.Second},
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

// fetch returns the plaintext tar for accessToken. 404 yields (nil, nil)
// (no snapshot yet). Retries transient (5xx, network) failures internally.
func (b *Buckets) fetch(ctx context.Context, bearer, accessToken, codeExecutionEncryptionKeyB64 string) ([]byte, error) {
	keyStd, err := urlBase64ToStd(codeExecutionEncryptionKeyB64)
	if err != nil {
		return nil, err
	}
	return httpRetry(ctx, bucketFetchAttempts, restorePushRetryDelay, func() ([]byte, bool, error) {
		return b.fetchOnce(ctx, bearer, accessToken, keyStd)
	})
}

// fetchOnce returns (bytes, transient, err). 404 → (nil, false, nil)
// signals "no snapshot, fall through to fresh workspace".
func (b *Buckets) fetchOnce(ctx context.Context, bearer, accessToken, keyStd string) ([]byte, bool, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", b.baseURL+"/items/"+accessToken, nil)
	if err != nil {
		return nil, false, err
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("X-Encryption-Key", keyStd)
	req.Header.Set("User-Agent", "tinfoil-orchestrator/1.0")

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return nil, true, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, false, nil
	}
	if resp.StatusCode >= 500 {
		return nil, true, fmt.Errorf("buckets GET: %d", resp.StatusCode)
	}
	if resp.StatusCode >= 400 {
		return nil, false, fmt.Errorf("buckets GET: %d", resp.StatusCode)
	}
	raw, err := readLimited(resp.Body, maxSnapshotBody)
	if err != nil {
		return nil, false, fmt.Errorf("buckets GET body: %w", err)
	}
	var body struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, false, fmt.Errorf("decode bucket response: %w", err)
	}
	tarBytes, err := base64.StdEncoding.DecodeString(body.Value)
	if err != nil {
		return nil, false, fmt.Errorf("decode bucket value: %w", err)
	}
	return tarBytes, false, nil
}

// put stores the plaintext tar; buckets encrypts it under the supplied key.
// Retries transient (5xx, network) failures internally.
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
	_, err = httpRetry(ctx, bucketPutAttempts, snapshotPutRetryDelay, func() (struct{}, bool, error) {
		return b.putOnce(ctx, bearer, accessToken, body)
	})
	return err
}

func (b *Buckets) putOnce(ctx context.Context, bearer, accessToken string, body []byte) (struct{}, bool, error) {
	req, err := http.NewRequestWithContext(ctx, "PUT", b.baseURL+"/items/"+accessToken, bytes.NewReader(body))
	if err != nil {
		return struct{}{}, false, err
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "tinfoil-orchestrator/1.0")

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return struct{}{}, true, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 500 {
		return struct{}{}, true, fmt.Errorf("buckets PUT: %d", resp.StatusCode)
	}
	if resp.StatusCode >= 400 {
		return struct{}{}, false, fmt.Errorf("buckets PUT: %d", resp.StatusCode)
	}
	return struct{}{}, false, nil
}

// ---------------------------------------------------------------------------
// Executor /restore + /snapshot — hits the per-container attested client.
// ---------------------------------------------------------------------------

// pushRestore POSTs the plaintext tar to the container's /restore.
// Retries transient (5xx, network) failures internally. recordContainerStatus
// is called on every attempt so 403s still count toward the threshold.
func (m *Manager) pushRestore(ctx context.Context, c *Container, plaintextTar []byte) error {
	if c.httpClient == nil {
		return fmt.Errorf("no http client for container %s", c.Name)
	}
	body, err := json.Marshal(map[string]string{
		"tar": base64.StdEncoding.EncodeToString(plaintextTar),
	})
	if err != nil {
		return err
	}
	_, err = httpRetry(ctx, pushRestoreAttempts, restorePushRetryDelay, func() (struct{}, bool, error) {
		return m.pushRestoreOnce(ctx, c, body)
	})
	return err
}

func (m *Manager) pushRestoreOnce(ctx context.Context, c *Container, body []byte) (struct{}, bool, error) {
	authToken := sessionContainerAuthToken(ctx)
	if authToken == "" {
		return struct{}{}, false, fmt.Errorf("missing container auth token on ctx for restore")
	}
	req, err := http.NewRequestWithContext(ctx, "POST", "https://"+c.Domain+"/restore", bytes.NewReader(body))
	if err != nil {
		return struct{}{}, false, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Code-Execution-Container-Auth-Token", authToken)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return struct{}{}, true, fmt.Errorf("restore POST: %w", err)
	}
	defer resp.Body.Close()
	_, _ = readLimited(resp.Body, maxExecutorBody)
	m.recordContainerStatus(c, resp.StatusCode)
	if resp.StatusCode >= 500 {
		return struct{}{}, true, fmt.Errorf("restore returned %d", resp.StatusCode)
	}
	if resp.StatusCode >= 400 {
		return struct{}{}, false, fmt.Errorf("restore returned %d", resp.StatusCode)
	}
	return struct{}{}, false, nil
}

// Streams a plaintext tar of /workspace.
// Clean stream is signaled by the X-Snapshot-Status: ok HTTP trailer;
// 200 with the trailer absent or != "ok" means there was a problem.
// /snapshot doesn't need an auth-token
func (m *Manager) fetchSnapshotFromContainer(ctx context.Context, c *Container) ([]byte, error) {
	if c.httpClient == nil {
		return nil, fmt.Errorf("no http client for container %s", c.Name)
	}
	req, err := http.NewRequestWithContext(ctx, "POST", "https://"+c.Domain+"/snapshot", bytes.NewReader([]byte("{}")))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("snapshot POST: %w", err)
	}
	defer resp.Body.Close()
	data, readErr := readLimited(resp.Body, maxSnapshotBody)
	m.recordContainerStatus(c, resp.StatusCode)
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
