package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/tinfoilsh/verifier/client"
)

// apiBase is the controlplane root for /api/* calls.
var apiBase = "https://api.tinfoil.sh"

type Container struct {
	ID         string
	Name       string
	Domain     string
	Status     string
	CreatedAt  time.Time
	AssignedAt time.Time
	SSHPort    int

	httpClient *http.Client // proxy client (attested or plain)

	// Needed at eviction time to call the container's /snapshot endpoint, which wraps the DEK to it.
	Pubkey string

	// LastActivity is updated on every successful proxied tool call.
	// The eviction loop uses it to decide when to snapshot+destroy.
	LastActivity time.Time
}

type ManagerConfig struct {
	AdminAPIKey       string
	PoolSize          int
	MaxContainers     int
	PollInterval      time.Duration
	ConfigRepo        string
	ConfigTag         string
	DebugMode         bool
	VerifyAttestation bool
	// IdleTimeout is how long a session can have no tool activity before
	// the orchestrator snapshots and evicts the container.
	IdleTimeout time.Duration
	// EvictionPoll is how often the eviction loop wakes up to scan
	// sessions. Roughly IdleTimeout / 10.
	EvictionPoll time.Duration
}

type Manager struct {
	cfg ManagerConfig

	mu           sync.Mutex
	cond         *sync.Cond
	warmPool     []*Container
	inflight     []*Container
	sessions     map[string]*Container
	assignLocks  map[string]*sync.Mutex // per-execSessionID serialization, see lockSession
	failed       []*Container
	failCount    int
	apiErrors    int
	shuttingDown bool

	apiClient *http.Client
}

func NewManager(cfg ManagerConfig) *Manager {
	if cfg.IdleTimeout == 0 {
		cfg.IdleTimeout = 10 * time.Minute
	}
	if cfg.EvictionPoll == 0 {
		cfg.EvictionPoll = 30 * time.Second
	}
	m := &Manager{
		cfg:         cfg,
		sessions:    map[string]*Container{},
		assignLocks: map[string]*sync.Mutex{},
		apiClient:   &http.Client{Timeout: 30 * time.Second},
	}
	m.cond = sync.NewCond(&m.mu)
	return m
}

// lockSession returns a mutex specific to execSessionID. Callers should
// hold it across the read-or-restore-then-assign critical section so that
// two concurrent webapp tabs hitting GetOrAssign with the same session
// don't both pull a fresh container from the warm pool. The first one
// wins; the second sees the in-memory hit and re-uses the assigned
// container. Locks are kept indefinitely (one per session, cheap) until
// CleanupSession drops them.
func (m *Manager) lockSession(sessionID string) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	if l, ok := m.assignLocks[sessionID]; ok {
		return l
	}
	l := &sync.Mutex{}
	m.assignLocks[sessionID] = l
	return l
}

// ---------------------------------------------------------------------------
// Controlplane API
// ---------------------------------------------------------------------------

func (m *Manager) apiRequest(method, path string, body any) (int, []byte, error) {
	var buf io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		buf = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, apiBase+path, buf)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+m.cfg.AdminAPIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "tinfoil-orchestrator/1.0")

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
		"name":     name,
		"repo":     m.cfg.ConfigRepo,
		"tag":      m.cfg.ConfigTag,
		"debug":    m.cfg.DebugMode,
		"ssh_keys": []string{"daniel"},
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
		ID      string `json:"id"`
		Domain  string `json:"domain"`
		SSHPort int    `json:"ssh_port"`
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
		SSHPort:   resp.SSHPort,
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

// ---------------------------------------------------------------------------
// Attestation / proxy client
// ---------------------------------------------------------------------------

func (m *Manager) buildProxyClient(c *Container) (*http.Client, error) {
	// TODO: flip VERIFY_ATTESTATION to true in prod alongside the snapshot
	// feature. Default stays false here so local/dev still work without
	// enclave attestation, but the chained-attestation trust model
	// (webapp -> router -> orchestrator -> container) requires this on
	// in production for the resume handshake to be meaningful.
	if !m.cfg.VerifyAttestation {
		log.Printf("orchestrator: skipping attestation for %s (%s)", c.Name, c.Domain)
		return &http.Client{Timeout: 35 * time.Second}, nil
	}
	sc := client.NewSecureClient(c.Domain, m.cfg.ConfigRepo)
	httpClient, err := sc.HTTPClient()
	if err != nil {
		return nil, err
	}
	httpClient.Timeout = 35 * time.Second
	log.Printf("orchestrator: attestation verified for %s (%s)", c.Name, c.Domain)
	return httpClient, nil
}

// ---------------------------------------------------------------------------
// Pool management
// ---------------------------------------------------------------------------

func (m *Manager) replenishPool() []*Container {
	m.mu.Lock()
	total := len(m.warmPool) + len(m.inflight) + len(m.sessions)
	target := len(m.sessions) + m.cfg.PoolSize
	if target > m.cfg.MaxContainers {
		target = m.cfg.MaxContainers
	}
	needed := target - total
	m.mu.Unlock()

	var created []*Container
	for i := 0; i < needed; i++ {
		c := m.createContainer()
		if c == nil {
			break
		}
		log.Printf("orchestrator: created container %s (%s)", c.Name, c.ID)
		created = append(created, c)
	}
	if len(created) > 0 {
		m.mu.Lock()
		m.inflight = append(m.inflight, created...)
		m.mu.Unlock()
	}
	return created
}

func (m *Manager) pollInflight() {
	m.mu.Lock()
	toPoll := append([]*Container(nil), m.inflight...)
	m.mu.Unlock()

	var ready, failed []*Container
	for _, c := range toPoll {
		s := m.pollContainer(c.ID)
		switch s {
		case "ready":
			cli, err := m.buildProxyClient(c)
			if err != nil {
				log.Printf("orchestrator: attestation failed for %s: %v", c.Name, err)
				failed = append(failed, c)
				continue
			}
			c.httpClient = cli
			c.Status = "ready"
			ready = append(ready, c)
			log.Printf("orchestrator: container %s is ready", c.Name)
		case "failed":
			failed = append(failed, c)
			log.Printf("orchestrator: container %s failed", c.Name)
		}
	}

	if len(ready) == 0 && len(failed) == 0 {
		return
	}

	m.mu.Lock()
	for _, c := range ready {
		m.removeFromInflight(c)
		m.warmPool = append(m.warmPool, c)
	}
	for _, c := range failed {
		m.removeFromInflight(c)
		c.Status = "failed"
		m.failed = append(m.failed, c)
		m.failCount++
	}
	for len(m.failed) > 10 {
		m.failed = m.failed[1:]
	}
	if len(ready) > 0 {
		m.cond.Broadcast()
	}
	m.mu.Unlock()
}

// must hold m.mu
func (m *Manager) removeFromInflight(c *Container) {
	for i, x := range m.inflight {
		if x == c {
			m.inflight = append(m.inflight[:i], m.inflight[i+1:]...)
			return
		}
	}
}

func (m *Manager) StartPoolManager() {
	go m.poolManagerLoop()
}

func (m *Manager) poolManagerLoop() {
	consecutiveFailures := 0
	for !m.shuttingDown {
		var created []*Container
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("orchestrator: pool manager error: %v", r)
				}
			}()
			if !m.shuttingDown {
				created = m.replenishPool()
			}
			m.pollInflight()
		}()

		m.mu.Lock()
		needed := (len(m.sessions) + m.cfg.PoolSize) - len(m.warmPool) - len(m.inflight) - len(m.sessions)
		if (len(m.sessions) + m.cfg.PoolSize) > m.cfg.MaxContainers {
			needed = m.cfg.MaxContainers - len(m.warmPool) - len(m.inflight) - len(m.sessions)
		}
		m.mu.Unlock()

		if needed > 0 && len(created) == 0 {
			consecutiveFailures++
		} else {
			consecutiveFailures = 0
		}

		mult := 1
		if consecutiveFailures > 0 {
			n := consecutiveFailures
			if n > 4 {
				n = 4
			}
			for i := 0; i < n; i++ {
				mult *= 2
			}
		}
		delay := m.cfg.PollInterval * time.Duration(mult)
		if delay > 30*time.Second {
			delay = 30 * time.Second
		}
		if consecutiveFailures > 0 && consecutiveFailures%5 == 1 {
			log.Printf("orchestrator: create failing, backoff %v (consecutive failures: %d)", delay, consecutiveFailures)
		}
		time.Sleep(delay)
	}
}

// ---------------------------------------------------------------------------
// Session management
// ---------------------------------------------------------------------------

// GetOrAssign returns an existing session's container or assigns one from
// the warm pool, blocking up to 60s. isConnected is consulted while waiting
// so a disconnected client can short-circuit.
//
// Per-execSessionId serialization: two concurrent tabs hitting this with
// the same session ID race on a per-session mutex (lockSession). Only one
// of them pulls a fresh container and runs restore; the other waits, then
// sees the in-memory hit and shares the same container. This prevents
// double-assignment and double-restore.
//
// Per-request attrs are read from ctx (see WithSessionAttrs):
//   - Pubkey is stamped onto c.Pubkey so the eviction loop can wrap a
//     snapshot to it long after the request returns. We refresh on every
//     call so a key rotation mid-session is picked up.
//   - ResumeDEK, when present and the controlplane has a bundle, drives
//     a fetch + decrypt + POST to the container's /restore BEFORE the
//     container is returned. The executor closes its restore gate on the
//     first non-/restore call, so this is the only safe point. The DEK
//     never outlives the request — it lives on ctx and disappears with it.
func (m *Manager) GetOrAssign(ctx context.Context, sessionID string, isConnected func() bool) (*Container, string) {
	// Take the per-session lock first. This is the serialization point.
	sl := m.lockSession(sessionID)
	sl.Lock()
	defer sl.Unlock()

	pubkey := sessionPubkey(ctx)

	m.mu.Lock()
	if c, ok := m.sessions[sessionID]; ok {
		if pubkey != "" {
			c.Pubkey = pubkey
		}
		m.mu.Unlock()
		return c, ""
	}
	if len(m.sessions) >= m.cfg.MaxContainers {
		m.mu.Unlock()
		return nil, fmt.Sprintf("at capacity (%d sessions)", m.cfg.MaxContainers)
	}

	deadline := time.Now().Add(60 * time.Second)
	for len(m.warmPool) == 0 {
		if time.Now().After(deadline) {
			m.mu.Unlock()
			return nil, "no containers available (timed out after 60s)"
		}
		if isConnected != nil && !isConnected() {
			m.mu.Unlock()
			log.Printf("orchestrator: client disconnected while waiting for session %s", sessionID)
			return nil, "client disconnected"
		}
		// wait up to 2s at a time so we can re-check connection / deadline
		go func() {
			time.Sleep(2 * time.Second)
			m.cond.Broadcast()
		}()
		m.cond.Wait()
	}

	c := m.warmPool[0]
	m.warmPool = m.warmPool[1:]
	c.Status = "assigning"
	c.AssignedAt = time.Now()
	c.LastActivity = time.Now()
	if pubkey != "" {
		c.Pubkey = pubkey
	}
	m.mu.Unlock()

	// Restore-on-assign. Failures here log and continue with an empty
	// workspace — better than refusing to assign and breaking the user's
	// chat entirely. (The webapp can surface a "restore failed" hint.)
	if dek := sessionResumeDEK(ctx); dek != "" {
		if err := m.restoreInto(sessionID, c, dek); err != nil {
			log.Printf("orchestrator: restore failed for session %s on %s: %v (continuing with empty workspace)",
				sessionID, c.Name, err)
		} else {
			log.Printf("orchestrator: restored snapshot for session %s onto %s", sessionID, c.Name)
		}
	}

	// Mark assigned only AFTER restore so the eviction loop can't trip
	// on a half-bootstrapped container.
	m.mu.Lock()
	c.Status = "assigned"
	m.sessions[sessionID] = c
	m.mu.Unlock()

	log.Printf("orchestrator: assigned %s to session %s", c.Name, sessionID)
	return c, ""
}

// restoreInto fetches the snapshot bundle for sessionID, decrypts the tar
// using the orchestrator-passed resume DEK, and pushes plaintext into
// the container's /restore. No-op if the controlplane has no bundle for
// this session (returns nil; we just keep the empty workspace).
func (m *Manager) restoreInto(sessionID string, c *Container, resumeDEKb64 string) error {
	bundle, err := m.fetchSnapshotBundle(sessionID)
	if err != nil {
		return fmt.Errorf("fetch bundle: %w", err)
	}
	if bundle == nil {
		// No snapshot in storage — fresh container, fine.
		return nil
	}
	ct, err := decodeBase64Lenient(bundle.Ciphertext)
	if err != nil {
		return fmt.Errorf("decode ciphertext: %w", err)
	}
	dek, err := decodeBase64Lenient(resumeDEKb64)
	if err != nil {
		return fmt.Errorf("decode dek: %w", err)
	}
	plain, err := decryptSnapshotTar(dek, ct)
	if err != nil {
		return fmt.Errorf("decrypt tar: %w", err)
	}
	if err := m.pushRestore(c, plain); err != nil {
		return fmt.Errorf("push restore: %w", err)
	}
	return nil
}

// decodeBase64Lenient accepts std, raw-std, url, or raw-url base64.
// We do this because callers (webapp, controlplane) sometimes pad and
// sometimes don't, and the snapshot wire format uses std encoding from
// the executor while DEK headers from the webapp tend to be url-safe.
func decodeBase64Lenient(s string) ([]byte, error) {
	for _, enc := range []*base64.Encoding{
		base64.StdEncoding,
		base64.RawStdEncoding,
		base64.URLEncoding,
		base64.RawURLEncoding,
	} {
		if b, err := enc.DecodeString(s); err == nil {
			return b, nil
		}
	}
	return nil, fmt.Errorf("not valid base64")
}

func (m *Manager) CleanupSession(sessionID string) *Container {
	m.mu.Lock()
	c, ok := m.sessions[sessionID]
	if ok {
		delete(m.sessions, sessionID)
	}
	delete(m.assignLocks, sessionID)
	m.mu.Unlock()
	if !ok {
		return nil
	}
	c.Status = "deleting"
	log.Printf("orchestrator: cleaning up %s for session %s", c.Name, sessionID)
	go m.deleteContainer(c.ID)
	return c
}

// evictAndSnapshot snapshots the container's workspace (if a pubkey was
// cached) and PUTs the bundle to controlplane, then deletes the
// container. Called by the idle-eviction loop. Snapshot failures still
// destroy the container — losing state is bad, but leaving stale
// containers around is worse and the user can always start fresh.
func (m *Manager) evictAndSnapshot(sessionID string, c *Container) {
	if c.Pubkey != "" {
		bundle, err := m.fetchSnapshotFromContainer(c, c.Pubkey)
		if err != nil {
			log.Printf("orchestrator: snapshot failed for session %s on %s: %v", sessionID, c.Name, err)
		} else if err := m.putSnapshotBundle(sessionID, bundle); err != nil {
			log.Printf("orchestrator: PUT snapshot bundle failed for session %s: %v", sessionID, err)
		} else {
			log.Printf("orchestrator: snapshotted session %s (container %s) to controlplane", sessionID, c.Name)
		}
	} else {
		log.Printf("orchestrator: no pubkey cached for session %s — skipping snapshot", sessionID)
	}
	c.Status = "deleting"
	m.deleteContainer(c.ID)
}

// StartEvictionLoop launches the background goroutine that scans
// sessions for idleness and snapshot+evicts any whose LastActivity is
// older than IdleTimeout. Idempotent intent: there's currently no
// guard against starting it twice, but main.go calls it exactly once.
func (m *Manager) StartEvictionLoop() {
	go m.evictionLoop()
}

func (m *Manager) evictionLoop() {
	for !m.shuttingDown {
		time.Sleep(m.cfg.EvictionPoll)
		if m.shuttingDown {
			return
		}
		m.evictIdleSessions()
	}
}

// evictIdleSessions snapshots each session whose LastActivity is older
// than IdleTimeout, then destroys the container. Walks the session
// map under the global lock to pick targets, releases the lock to do
// the (slow) snapshot+PUT+delete dance per target.
func (m *Manager) evictIdleSessions() {
	type target struct {
		sessionID string
		c         *Container
	}
	now := time.Now()
	var targets []target

	m.mu.Lock()
	for sid, c := range m.sessions {
		if now.Sub(c.LastActivity) >= m.cfg.IdleTimeout {
			targets = append(targets, target{sid, c})
		}
	}
	for _, t := range targets {
		// Drop the session entry but keep the assign lock to serialize
		// against any in-flight GetOrAssign for the same session: a
		// request arriving mid-eviction either sees the deletion and
		// assigns a fresh container, or queues behind the evictor cleanly.
		delete(m.sessions, t.sessionID)
	}
	m.mu.Unlock()

	for _, t := range targets {
		log.Printf("orchestrator: idle-evicting session %s on %s (idle ~%v)",
			t.sessionID, t.c.Name, m.cfg.IdleTimeout)
		m.evictAndSnapshot(t.sessionID, t.c)
		m.mu.Lock()
		delete(m.assignLocks, t.sessionID)
		m.mu.Unlock()
	}
}

func (m *Manager) CleanupAll() map[string]any {
	m.mu.Lock()
	all := append([]*Container{}, m.warmPool...)
	all = append(all, m.inflight...)
	for _, c := range m.sessions {
		all = append(all, c)
	}
	m.warmPool = nil
	m.inflight = nil
	m.sessions = map[string]*Container{}
	m.assignLocks = map[string]*sync.Mutex{}
	m.failed = nil
	m.mu.Unlock()

	deleted := []string{}
	skipped := []map[string]string{}
	for _, c := range all {
		if !m.verifyContainerName(c) {
			skipped = append(skipped, map[string]string{"name": c.Name, "id": c.ID, "reason": "verification failed"})
			continue
		}
		log.Printf("orchestrator: deleting %s (%s) — verified", c.Name, c.ID)
		m.deleteContainer(c.ID)
		deleted = append(deleted, c.Name)
	}
	return map[string]any{"deleted": deleted, "skipped": skipped, "count": len(deleted)}
}

func (m *Manager) Finish() map[string]any {
	m.shuttingDown = true
	log.Printf("orchestrator: finishing — deleting all containers and shutting down")
	result := m.CleanupAll()
	result["status"] = "finished"
	return result
}

// ---------------------------------------------------------------------------
// Container proxy + high-level operations
// ---------------------------------------------------------------------------

func (m *Manager) proxy(c *Container, path string, body []byte) (int, []byte, error) {
	if c.httpClient == nil {
		return 0, nil, fmt.Errorf("no http client for container %s", c.Name)
	}
	url := "https://" + c.Domain + path
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 502, []byte(fmt.Sprintf(`{"error":"container unavailable: %s"}`, err)), nil
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	// Record activity on every reachable proxy hop. The eviction loop
	// uses this to compute idleness, so we want it bumped even on
	// non-2xx responses (the user is still interacting with us).
	c.LastActivity = time.Now()
	return resp.StatusCode, data, nil
}

// ExecCommand runs a bash command in the session's container.
func (m *Manager) ExecCommand(ctx context.Context, sessionID, command string) map[string]any {
	c, errMsg := m.GetOrAssign(ctx, sessionID, nil)
	if c == nil {
		return map[string]any{"error": errMsg}
	}
	body, _ := json.Marshal(map[string]string{"command": command})
	status, raw, _ := m.proxy(c, "/exec", body)
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return map[string]any{"error": fmt.Sprintf("proxy returned status %d", status), "raw": string(raw)}
	}
	return out
}

// ReadFile reads a text file from the session's container.
func (m *Manager) ReadFile(ctx context.Context, sessionID, path string) (string, error) {
	c, errMsg := m.GetOrAssign(ctx, sessionID, nil)
	if c == nil {
		return "", fmt.Errorf("%s", errMsg)
	}
	body, _ := json.Marshal(map[string]string{"path": path})
	_, raw, _ := m.proxy(c, "/read", body)
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
func (m *Manager) WriteFile(ctx context.Context, sessionID, path, content string) map[string]any {
	c, errMsg := m.GetOrAssign(ctx, sessionID, nil)
	if c == nil {
		return map[string]any{"error": errMsg}
	}
	encoded := base64.StdEncoding.EncodeToString([]byte(content))
	body, _ := json.Marshal(map[string]string{"path": path, "contents": encoded})
	status, raw, _ := m.proxy(c, "/write", body)
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return map[string]any{"error": fmt.Sprintf("proxy returned status %d", status)}
	}
	return out
}

// FileExists returns true iff the file is readable.
func (m *Manager) FileExists(ctx context.Context, sessionID, path string) bool {
	_, err := m.ReadFile(ctx, sessionID, path)
	return err == nil
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

func (m *Manager) HealthInfo() map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	return map[string]any{
		"status":         "ok",
		"warm_pool":      len(m.warmPool),
		"inflight":       len(m.inflight),
		"sessions":       len(m.sessions),
		"pool_target":    m.cfg.PoolSize,
		"max_containers": m.cfg.MaxContainers,
	}
}

func (m *Manager) MetricsInfo() map[string]any {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()

	rec := func(c *Container, sessionID string) map[string]any {
		d := map[string]any{
			"id":       c.ID,
			"name":     c.Name,
			"status":   c.Status,
			"uptime":   int(now.Sub(c.CreatedAt).Seconds()),
			"ssh_port": c.SSHPort,
		}
		if sessionID != "" {
			d["session_id"] = sessionID
			active := 0
			if !c.AssignedAt.IsZero() {
				active = int(now.Sub(c.AssignedAt).Seconds())
			}
			d["active_time"] = active
		}
		return d
	}

	warm := []map[string]any{}
	for _, c := range m.warmPool {
		warm = append(warm, rec(c, ""))
	}
	inflight := []map[string]any{}
	for _, c := range m.inflight {
		inflight = append(inflight, rec(c, ""))
	}
	sessions := []map[string]any{}
	for sid, c := range m.sessions {
		sessions = append(sessions, rec(c, sid))
	}
	failed := []map[string]any{}
	for _, c := range m.failed {
		failed = append(failed, rec(c, ""))
	}

	return map[string]any{
		"warm_pool":      warm,
		"inflight":       inflight,
		"sessions":       sessions,
		"failed":         failed,
		"fail_count":     m.failCount,
		"api_errors":     m.apiErrors,
		"pool_target":    m.cfg.PoolSize,
		"max_containers": m.cfg.MaxContainers,
	}
}
