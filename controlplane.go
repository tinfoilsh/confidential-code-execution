package main

// container CRUD + status polling.

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"time"
)

func (m *Manager) apiRequest(method, path string, body any) (int, []byte, error) {
	return m.apiRequestWithHeaders(method, path, body, nil)
}

func (m *Manager) apiRequestWithHeaders(method, path string, body any, extra map[string]string) (int, []byte, error) {
	var buf io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		buf = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, m.cfg.ControlPlaneURL+path, buf)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+m.cfg.AdminAPIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "tinfoil-orchestrator/1.0")
	for k, v := range extra {
		req.Header.Set(k, v)
	}

	resp, err := m.apiClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		log.Printf("orchestrator: API error %d %s %s: %s", resp.StatusCode, method, path, data)
	}
	return resp.StatusCode, data, nil
}

func (m *Manager) createContainer() *Container {
	suffix := make([]byte, 4)
	if _, err := rand.Read(suffix); err != nil {
		return nil
	}
	name := "daniel-exec-" + hex.EncodeToString(suffix)
	body := map[string]any{
		"name": name,
		"repo": m.cfg.EnvironmentRepo,
		"tag":  m.cfg.EnvironmentTag,
	}
	status, raw, err := m.apiRequest("POST", "/api/containers", body)
	if err != nil || status != 201 {
		log.Printf("orchestrator: failed to create container %s: %d %v", name, status, err)
		m.mu.Lock()
		m.apiErrors++
		m.mu.Unlock()
		return nil
	}
	var resp struct {
		ID     string `json:"id"`
		Domain string `json:"domain"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil
	}
	return &Container{
		ID:        resp.ID,
		Name:      name,
		Domain:    resp.Domain,
		Status:    "deploying",
		CreatedAt: time.Now(),
	}
}

func (m *Manager) pollContainer(id string) string {
	status, raw, err := m.apiRequest("GET", "/api/containers/"+id, nil)
	if err != nil || status != 200 {
		return ""
	}
	var resp struct {
		Status string `json:"status"`
	}
	json.Unmarshal(raw, &resp)
	return resp.Status
}

func (m *Manager) deleteContainer(id string) {
	m.apiRequest("DELETE", "/api/containers/"+id, nil)
}

// verifyContainerName guards CleanupAll against deleting a container
// whose ID has been reused under a different name on the controlplane.
func (m *Manager) verifyContainerName(c *Container) bool {
	status, raw, err := m.apiRequest("GET", "/api/containers/"+c.ID, nil)
	if err != nil || status != 200 {
		log.Printf("orchestrator: skip delete %s (%s) — not found on controlplane (%d)", c.Name, c.ID, status)
		return false
	}
	var resp struct {
		Name string `json:"name"`
	}
	json.Unmarshal(raw, &resp)
	if resp.Name != c.Name {
		log.Printf("orchestrator: skip delete %s (%s) — name mismatch: remote=%s", c.Name, c.ID, resp.Name)
		return false
	}
	return true
}
