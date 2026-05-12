package main

// HTTP client for the executor (the api-server inside each container):
// builds the attested TLS client, proxies tool calls, and enforces the
// poisoned-container 403 threshold.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

// 90s ceiling covers snapshot/restore bulk transfers; per-call
// /exec etc. hold to the tighter toolCallTimeout via per-request ctx so
// a runaway tool can't tie up a session for the full 90s.
func (m *Manager) buildProxyClient(c *Container) (*http.Client, error) {
	// attested client
	httpClient, err := executorHTTPClient(c.Domain, m.cfg.EnvironmentRepo)
	if err != nil {
		return nil, err
	}
	httpClient.Timeout = 90 * time.Second
	return httpClient, nil
}

func (m *Manager) proxy(ctx context.Context, c *Container, path string, body []byte) (int, []byte, error) {
	if c.httpClient == nil {
		return 0, nil, fmt.Errorf("no http client for container %s", c.Name)
	}
	authToken := sessionContainerAuthToken(ctx)
	if authToken == "" {
		return 0, nil, fmt.Errorf("missing container auth token on ctx for %s %s", c.Name, path)
	}
	callCtx, cancel := context.WithTimeout(ctx, toolCallTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(callCtx, "POST", "https://"+c.Domain+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Code-Execution-Container-Auth-Token", authToken)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		log.Printf("orchestrator: proxy %s %s: %v", c.Name, path, err)
		body, _ := json.Marshal(map[string]string{"error": "container unavailable"})
		return 502, body, nil
	}
	defer resp.Body.Close()
	data, readErr := readLimited(resp.Body, maxExecutorBody)
	// Bump activity even on non-2xx — the user is still interacting.
	c.bumpActivity()
	m.recordContainerStatus(c, resp.StatusCode)
	if readErr != nil {
		return resp.StatusCode, nil, readErr
	}
	return resp.StatusCode, data, nil
}

// recordContainerStatus reacts to the executor's auth-token gate.
// A 403 deterministically means our auth token doesn't match what the
// container's gate locked to — no retry can fix it without changing
// the token, and the token is HKDF-derived from session identity, so
// we destroy the session immediately on first 403.
func (m *Manager) recordContainerStatus(c *Container, status int) {
	if status != http.StatusForbidden {
		return
	}
	c.mu.Lock()
	accessToken := c.AccessToken
	c.mu.Unlock()
	log.Printf("orchestrator: container %s rejected the session's auth token — destroying", c.Name)
	// accessToken is "" until the container is committed in GetOrAssign;
	// during restore CleanupSession("") is a safe no-op.
	m.CleanupSession(accessToken)
}

// ExecCommand runs a bash command in the session's container.
func (m *Manager) ExecCommand(ctx context.Context, accessToken, command string) map[string]any {
	c, errMsg := m.GetOrAssign(ctx, accessToken, nil)
	if c == nil {
		return map[string]any{"error": errMsg}
	}
	body, _ := json.Marshal(map[string]string{"command": command})
	status, raw, _ := m.proxy(ctx, c, "/exec", body)
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return map[string]any{"error": fmt.Sprintf("proxy returned status %d", status)}
	}
	return out
}

// ReadFile reads a text file from the session's container.
func (m *Manager) ReadFile(ctx context.Context, accessToken, path string) (string, error) {
	c, errMsg := m.GetOrAssign(ctx, accessToken, nil)
	if c == nil {
		return "", fmt.Errorf("%s", errMsg)
	}
	body, _ := json.Marshal(map[string]string{"path": path})
	_, raw, _ := m.proxy(ctx, c, "/read", body)
	var resp struct {
		Contents string `json:"contents"`
		Error    string `json:"error"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return "", err
	}
	if resp.Error != "" {
		return "", fmt.Errorf("%s", resp.Error)
	}
	decoded, err := base64.StdEncoding.DecodeString(resp.Contents)
	if err != nil {
		return "", err
	}
	return string(decoded), nil
}

// WriteFile writes text content to a file on the session's container.
func (m *Manager) WriteFile(ctx context.Context, accessToken, path, content string) map[string]any {
	c, errMsg := m.GetOrAssign(ctx, accessToken, nil)
	if c == nil {
		return map[string]any{"error": errMsg}
	}
	encoded := base64.StdEncoding.EncodeToString([]byte(content))
	body, _ := json.Marshal(map[string]string{"path": path, "contents": encoded})
	status, raw, _ := m.proxy(ctx, c, "/write", body)
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return map[string]any{"error": fmt.Sprintf("proxy returned status %d", status)}
	}
	return out
}

// FileExists returns true iff the file is readable.
func (m *Manager) FileExists(ctx context.Context, accessToken, path string) bool {
	_, err := m.ReadFile(ctx, accessToken, path)
	return err == nil
}
