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

	"github.com/tinfoilsh/verifier/client"
)

// 90s ceiling covers snapshot/restore bulk transfers; per-call /exec etc.
// hold to the tighter toolCallTimeout via per-request ctx so a runaway
// tool can't tie up a session for the full 90s.
func (m *Manager) buildProxyClient(c *Container) (*http.Client, error) {
	if m.cfg.DevSkipAttestation {
		log.Printf("orchestrator: WARNING attestation disabled (DEV_SKIP_ATTESTATION) for %s", c.Name)
		return &http.Client{Timeout: 90 * time.Second}, nil
	}
	sc := client.NewSecureClient(c.Domain, m.cfg.EnvironmentRepo)
	httpClient, err := sc.HTTPClient()
	if err != nil {
		return nil, err
	}
	httpClient.Timeout = 90 * time.Second
	return httpClient, nil
}

func (m *Manager) proxy(ctx context.Context, c *Container, accessToken, path string, body []byte) (int, []byte, error) {
	if c.httpClient == nil {
		return 0, nil, fmt.Errorf("no http client for container %s", c.Name)
	}
	callCtx, cancel := context.WithTimeout(ctx, toolCallTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(callCtx, "POST", "https://"+c.Domain+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Code-Execution-Access-Token", accessToken)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 502, []byte(fmt.Sprintf(`{"error":"container unavailable: %s"}`, err)), nil
	}
	defer resp.Body.Close()
	data, readErr := readLimited(resp.Body, maxExecutorBody)
	// Bump activity even on non-2xx — the user is still interacting.
	c.LastActivity = time.Now()
	m.recordContainerStatus(accessToken, c, resp.StatusCode)
	if readErr != nil {
		return resp.StatusCode, nil, readErr
	}
	return resp.StatusCode, data, nil
}

// recordContainerStatus tracks back-to-back 403s from the executor's
// access-token gate. Two in a row means the container has a different
// access token claimed than what we're sending — almost certainly a
// poisoned warm-pool container or session-map drift, so we destroy the
// session. Non-403 4xx/5xx are ignored: not evidence of token mismatch.
func (m *Manager) recordContainerStatus(accessToken string, c *Container, status int) {
	if status >= 200 && status < 300 {
		m.mu.Lock()
		c.Consecutive403s = 0
		m.mu.Unlock()
		return
	}
	if status != http.StatusForbidden {
		return
	}
	m.mu.Lock()
	c.Consecutive403s++
	n := c.Consecutive403s
	m.mu.Unlock()
	if n >= 2 {
		log.Printf("orchestrator: container %s rejected the session's access token twice — destroying", c.Name)
		m.CleanupSession(accessToken)
	}
}

// ExecCommand runs a bash command in the session's container.
func (m *Manager) ExecCommand(ctx context.Context, accessToken, command string) map[string]any {
	c, errMsg := m.GetOrAssign(ctx, accessToken, nil)
	if c == nil {
		return map[string]any{"error": errMsg}
	}
	body, _ := json.Marshal(map[string]string{"command": command})
	status, raw, _ := m.proxy(ctx, c, accessToken, "/exec", body)
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
	_, raw, _ := m.proxy(ctx, c, accessToken, "/read", body)
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
	status, raw, _ := m.proxy(ctx, c, accessToken, "/write", body)
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
