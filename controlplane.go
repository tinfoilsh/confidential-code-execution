package main

// HTTP client for api.tinfoil.sh: container CRUD + api_key validation.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

type Controlplane struct {
	baseURL    string
	scopedCodeExecAdminKey   string
	httpClient *http.Client
}

func NewControlplane(baseURL, scopedCodeExecAdminKey string) *Controlplane {
	return &Controlplane{
		baseURL:    baseURL,
		scopedCodeExecAdminKey:   scopedCodeExecAdminKey,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

func (cp *Controlplane) do(ctx context.Context, method, path string, body any) (int, []byte, error) {
	var buf io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		buf = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, cp.baseURL+path, buf)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+cp.scopedCodeExecAdminKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "tinfoil-orchestrator/1.0")

	resp, err := cp.httpClient.Do(req)
	if err != nil {
		controlplaneErrors.Inc()
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, err := readLimited(resp.Body, maxControlplaneBody)
	if resp.StatusCode >= 500 {
		controlplaneErrors.Inc()
	}
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, data, nil
}

// createContainer requests a new container and returns it with
// Status="deploying". Caller polls until "ready".
func (cp *Controlplane) createContainer(repo, tag string) (*Container, error) {
	suffix := make([]byte, 4)
	if _, err := rand.Read(suffix); err != nil {
		return nil, err
	}
	name := "code-exec-" + hex.EncodeToString(suffix)
	body := map[string]any{
		"name": name,
		"repo": repo,
		"tag":  tag,
	}
	status, raw, err := cp.do(context.Background(), "POST", "/api/containers", body)
	if err != nil {
		return nil, fmt.Errorf("create container %s: %w", name, err)
	}
	if status != 201 {
		return nil, fmt.Errorf("create container %s: status %d", name, status)
	}
	var resp struct {
		ID     string `json:"id"`
		Domain string `json:"domain"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, err
	}
	containersCreated.Inc()
	return &Container{
		ID:        resp.ID,
		Name:      name,
		Domain:    resp.Domain,
		Status:    "deploying",
		CreatedAt: time.Now(),
	}, nil
}

func (cp *Controlplane) pollContainer(id string) string {
	status, raw, err := cp.do(context.Background(), "GET", "/api/containers/"+id, nil)
	if err != nil || status != 200 {
		return ""
	}
	var resp struct {
		Status string `json:"status"`
	}
	json.Unmarshal(raw, &resp)
	return resp.Status
}

func (cp *Controlplane) deleteContainer(id string) {
	containersDeleted.Inc()
	cp.do(context.Background(), "DELETE", "/api/containers/"+id, nil)
}

// verifyContainerName guards CleanupAll against deleting a container
// whose ID has been reused under a different name on the controlplane.
func (cp *Controlplane) verifyContainerName(c *Container) bool {
	status, raw, err := cp.do(context.Background(), "GET", "/api/containers/"+c.ID, nil)
	if err != nil || status != 200 {
		return false
	}
	var resp struct {
		Name string `json:"name"`
	}
	json.Unmarshal(raw, &resp)
	if resp.Name != c.Name {
		log.Printf("orchestrator: skip delete %s — controlplane name mismatch", c.ID)
		return false
	}
	return true
}

func (cp *Controlplane) validateKey(ctx context.Context, apiKey string) (int, error) {
	body, err := json.Marshal(map[string]string{"api_key": apiKey})
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", cp.baseURL+"/api/shim/validate-key", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "tinfoil-orchestrator/1.0")

	resp, err := cp.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("controlplane validate-key: %w", err)
	}
	defer resp.Body.Close()
	return resp.StatusCode, nil
}

// ---------------------------------------------------------------------------
// Authentication
// ---------------------------------------------------------------------------

var ErrAuthRequired = errors.New("code execution requires a valid api key")

const (
	// Bounds how long a positive validate-key response is reused.
	authCacheTTL        = 30 * time.Second
	authCacheMaxEntries = 1024
)

func (m *Manager) authCacheCheck(bearer string) bool {
	m.authCacheMu.Lock()
	defer m.authCacheMu.Unlock()
	exp, ok := m.authCache[bearer]
	if !ok {
		return false
	}
	if time.Now().After(exp) {
		delete(m.authCache, bearer)
		return false
	}
	return true
}

// If the cache is at capacity, sweep expired entries first
func (m *Manager) authCacheStore(bearer string) {
	m.authCacheMu.Lock()
	defer m.authCacheMu.Unlock()
	if len(m.authCache) >= authCacheMaxEntries {
		now := time.Now()
		for k, exp := range m.authCache {
			if now.After(exp) {
				delete(m.authCache, k)
			}
		}
		for k := range m.authCache {
			if len(m.authCache) < authCacheMaxEntries {
				break
			}
			delete(m.authCache, k)
		}
	}
	m.authCache[bearer] = time.Now().Add(authCacheTTL)
}

func extractBearer(authHeader string) string {
	if !strings.HasPrefix(authHeader, "Bearer ") {
		return ""
	}
	return strings.TrimPrefix(authHeader, "Bearer ")
}

// ---------------------------------------------------------------------------
// User identity
// stashed on ctx by mcp.go, read by Manager onto *Container, used for storage
// ---------------------------------------------------------------------------

type ctxKey int

const (
	ctxKeyCodeExecutionEncryptionKey ctxKey = iota
	ctxKeyBearer
	ctxKeyContainerAuthToken
)

func WithCodeExecutionEncryptionKey(ctx context.Context, key string) context.Context {
	if key != "" {
		ctx = context.WithValue(ctx, ctxKeyCodeExecutionEncryptionKey, key)
	}
	return ctx
}

func sessionCodeExecutionEncryptionKey(ctx context.Context) string {
	v, _ := ctx.Value(ctxKeyCodeExecutionEncryptionKey).(string)
	return v
}

func WithBearer(ctx context.Context, bearer string) context.Context {
	if bearer != "" {
		ctx = context.WithValue(ctx, ctxKeyBearer, bearer)
	}
	return ctx
}

func sessionBearer(ctx context.Context) string {
	v, _ := ctx.Value(ctxKeyBearer).(string)
	return v
}

// Per-request only
func WithContainerAuthToken(ctx context.Context, token string) context.Context {
	if token != "" {
		ctx = context.WithValue(ctx, ctxKeyContainerAuthToken, token)
	}
	return ctx
}

func sessionContainerAuthToken(ctx context.Context) string {
	v, _ := ctx.Value(ctxKeyContainerAuthToken).(string)
	return v
}
