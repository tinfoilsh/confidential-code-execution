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

// apiBase is the controlplane root for /api/* calls. Overridable via
// CONTROL_PLANE_URL env var
var apiBase = envStr("CONTROL_PLANE_URL", "https://api.tinfoil.sh")

// snapshotPutRetryDelay is how long evictAndSnapshot waits between
// the first and second attempt to PUT a snapshot tar to buckets.
// Variable so tests can shrink it.
var snapshotPutRetryDelay = 1 * time.Second

// restorePushRetryDelay is the pause between the first and second
// pushRestore attempt when the first sees a transient failure. The
// executor's api-server keeps the restore window open across multiple
// /restore calls (the token gate also tolerates same-token retries), so
// a single retry is safe to layer on top. Variable so tests can shrink it.
var restorePushRetryDelay = 1 * time.Second

// toolCallTimeout caps individual /exec, /read, and /write calls. The
// container http.Client has a 90s ceiling for snapshot/restore bulk
// transfers; this shorter budget keeps tool calls bounded so a runaway
// bash command doesn't tie up a session for the full 90s.
var toolCallTimeout = 35 * time.Second

type Container struct {
	ID         string
	Name       string
	Domain     string
	Status     string
	CreatedAt  time.Time
	AssignedAt time.Time

	httpClient *http.Client // proxy client (attested or plain)

	// CodeExecutionEncryptionKey is the user's symmetric AES-256 key
	// (base64-encoded), cached from the request's
	// X-Code-Execution-Encryption-Key header. The orchestrator uses it at
	// eviction time to PUT the workspace tar to buckets under this key.
	// The container itself never sees this value.
	CodeExecutionEncryptionKey string

	// Bearer is the api_key from the request's Authorization header, cached
	// for use as the buckets bearer at restore-on-assign and eviction-time
	// snapshot. Refreshed on every tools/call so a key rotation mid-session
	// is picked up. Buckets resolves it to (user_id, org_id) for the
	// storage prefix.
	Bearer string

	// LastActivity is updated on every successful proxied tool call.
	// The eviction loop uses it to decide when to snapshot+destroy.
	LastActivity time.Time

	// Consecutive403s counts back-to-back 403 responses from the
	// executor's api-server token gate. Two in a row means the container
	// has a different access token claimed than what we're sending —
	// almost certainly a poisoned warm-pool container or session-map
	// drift. recordContainerStatus tears it down at the threshold.
	// Reset on any 2xx. Guarded by Manager.mu.
	Consecutive403s int

	// HealthFailures counts back-to-back /health failures observed by
	// the periodic health checker. Reaching MaxHealthFailures evicts the
	// container from the warm pool and destroys it.
	// Reset on any 200. Guarded by m.mu.
	HealthFailures int
}

type ManagerConfig struct {
	AdminAPIKey       string
	PoolSize          int
	MaxContainers     int
	PollInterval      time.Duration
	EnvironmentRepo   string
	EnvironmentTag    string
	// DevSkipAttestation skips enclave attestation. Local dev only.
	DevSkipAttestation bool
	// DevBypassAuth skips api_key validation. Local dev only.
	DevBypassAuth bool
	// IdleTimeout is how long a session can have no tool activity before
	// the orchestrator snapshots and evicts the container.
	IdleTimeout time.Duration
	// EvictionPoll is how often the eviction loop wakes up to scan
	// sessions. Roughly IdleTimeout / 10.
	EvictionPoll time.Duration
	// WarmPoolWaitTimeout caps how long GetOrAssign blocks waiting for a
	// container when the warm pool is empty. Past the cap, the call
	// returns "no containers available" so the client gets a fast error
	// instead of an indefinite spinner.
	WarmPoolWaitTimeout time.Duration
	// HealthCheckInterval is how often the background health checker
	// scans every warm container's /health. Independent of the pool manager loop
	HealthCheckInterval time.Duration
	// MaxHealthFailures is the consecutive-failure threshold at which
	// the health checker yanks a warm container from the pool and
	// destroys it.
	MaxHealthFailures int
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
		cfg.IdleTimeout = 1 * time.Minute
	}
	if cfg.EvictionPoll == 0 {
		cfg.EvictionPoll = 30 * time.Second
	}
	if cfg.WarmPoolWaitTimeout == 0 {
		cfg.WarmPoolWaitTimeout = 10 * time.Second
	}
	if cfg.HealthCheckInterval == 0 {
		cfg.HealthCheckInterval = 15 * time.Second
	}
	if cfg.MaxHealthFailures == 0 {
		cfg.MaxHealthFailures = 3
	}
	m := &Manager{
		cfg:         cfg,
		sessions:    map[string]*Container{},
		assignLocks: map[string]*sync.Mutex{},
		// 120s budget covers snapshot PUT/GET against controlplane at the
		// /workspace tmpfs ceiling: 512 MB plaintext → ~683 MB after base64
		// encoding into the JSON body. Container CRUD calls also share this
		// client but finish in well under a second, so the longer cap
		// doesn't affect them.
		apiClient: &http.Client{Timeout: 120 * time.Second},
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
func (m *Manager) lockSession(accessToken string) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	if l, ok := m.assignLocks[accessToken]; ok {
		return l
	}
	l := &sync.Mutex{}
	m.assignLocks[accessToken] = l
	return l
}

// ---------------------------------------------------------------------------
// Controlplane API
// ---------------------------------------------------------------------------

func (m *Manager) apiRequest(method, path string, body any) (int, []byte, error) {
	return m.apiRequestWithHeaders(method, path, body, nil)
}

// apiRequestWithHeaders is apiRequest plus caller-controlled headers.
func (m *Manager) apiRequestWithHeaders(method, path string, body any, extra map[string]string) (int, []byte, error) {
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
	// 90s ceiling on the container client covers snapshot/restore bulk
	// transfers at the /workspace tmpfs ceiling. /exec, /read, /write
	// hold to the tighter toolCallTimeout (35s) via per-request context,
	// so a runaway tool call can't tie up a session for the full 90s.
	if m.cfg.DevSkipAttestation {
		log.Printf("orchestrator: skipping attestation for %s (%s)", c.Name, c.Domain)
		return &http.Client{Timeout: 90 * time.Second}, nil
	}
	sc := client.NewSecureClient(c.Domain, m.cfg.EnvironmentRepo)
	httpClient, err := sc.HTTPClient()
	if err != nil {
		return nil, err
	}
	httpClient.Timeout = 90 * time.Second
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
// Pre-assign /health: warm containers can die between the periodic
// health pass and now (process crash, exec.sock disappearing, etc.).
// We probe /health right after popping; a non-200 means we discard
// the candidate and pop again.
//
// The user's Code Execution Encryption Key (read from ctx via
// sessionCodeExecutionEncryptionKey) is cached on
// c.CodeExecutionEncryptionKey so the eviction loop can PUT the snapshot
// to buckets under it long after the request returns. We refresh on
// every call so a key rotation mid-session is picked up. When a key is
// present at assign time we also try to restore from buckets — buckets
// returns 404 if no snapshot exists yet, in which case we proceed with
// an empty workspace.
func (m *Manager) GetOrAssign(ctx context.Context, accessToken string, isConnected func() bool) (*Container, string) {
	// Take the per-session lock first. This is the serialization point.
	sl := m.lockSession(accessToken)
	sl.Lock()
	defer sl.Unlock()

	codeExecutionEncryptionKey := sessionCodeExecutionEncryptionKey(ctx)
	bearer := sessionBearer(ctx)

	m.mu.Lock()
	if c, ok := m.sessions[accessToken]; ok {
		if codeExecutionEncryptionKey != "" {
			c.CodeExecutionEncryptionKey = codeExecutionEncryptionKey
		}
		if bearer != "" {
			c.Bearer = bearer
		}
		m.mu.Unlock()
		return c, ""
	}
	if len(m.sessions) >= m.cfg.MaxContainers {
		m.mu.Unlock()
		return nil, fmt.Sprintf("at capacity (%d sessions)", m.cfg.MaxContainers)
	}
	m.mu.Unlock()

	deadline := time.Now().Add(m.cfg.WarmPoolWaitTimeout)
	var c *Container
	for {
		m.mu.Lock()
		for len(m.warmPool) == 0 {
			if time.Now().After(deadline) {
				m.mu.Unlock()
				return nil, fmt.Sprintf("no containers available (timed out after %v)", m.cfg.WarmPoolWaitTimeout)
			}
			if isConnected != nil && !isConnected() {
				m.mu.Unlock()
				log.Printf("orchestrator: client disconnected while waiting for session %s", accessToken)
				return nil, "client disconnected"
			}
			// wait up to 2s at a time so we can re-check connection / deadline
			go func() {
				time.Sleep(2 * time.Second)
				m.cond.Broadcast()
			}()
			m.cond.Wait()
		}
		candidate := m.warmPool[0]
		m.warmPool = m.warmPool[1:]
		m.mu.Unlock()

		if m.checkContainerHealth(candidate) {
			c = candidate
			break
		}
		log.Printf("orchestrator: warm container %s failed pre-assign /health, discarding", candidate.Name)
		candidate.Status = "failed"
		go m.deleteContainer(candidate.ID)
	}

	m.mu.Lock()
	c.Status = "assigning"
	c.AssignedAt = time.Now()
	c.LastActivity = time.Now()
	if codeExecutionEncryptionKey != "" {
		c.CodeExecutionEncryptionKey = codeExecutionEncryptionKey
	}
	if bearer != "" {
		c.Bearer = bearer
	}
	m.mu.Unlock()

	// Restore-on-assign. Failures here log and continue with an empty
	// workspace — better than refusing to assign and breaking the user's
	// chat entirely.
	if codeExecutionEncryptionKey != "" && bearer != "" {
		if err := m.restoreInto(ctx, bearer, accessToken, c, codeExecutionEncryptionKey); err != nil {
			log.Printf("orchestrator: restore failed for session %s on %s: %v (continuing with empty workspace)",
				accessToken, c.Name, err)
		} else {
			log.Printf("orchestrator: restore complete for session %s on %s", accessToken, c.Name)
		}
	}

	// Mark assigned only AFTER restore so the eviction loop can't trip
	// on a half-bootstrapped container.
	m.mu.Lock()
	c.Status = "assigned"
	m.sessions[accessToken] = c
	m.mu.Unlock()

	log.Printf("orchestrator: assigned %s to session %s", c.Name, accessToken)
	return c, ""
}

// restoreInto fetches the snapshot for accessToken from buckets (which
// decrypts under the supplied Code Execution Encryption Key) and pushes
// the plaintext tar into the container's /restore. No-op when the
// bucket has no entry, or when the supplied key can't open the entry —
// both surface as a fresh empty workspace.
func (m *Manager) restoreInto(ctx context.Context, bearer, accessToken string, c *Container, codeExecutionEncryptionKeyB64 string) error {
	tarBytes, err := m.fetchSnapshotTar(ctx, bearer, accessToken, codeExecutionEncryptionKeyB64)
	if err != nil {
		return fmt.Errorf("fetch snapshot: %w", err)
	}
	if tarBytes == nil {
		// No snapshot (or unreadable with this key) — fresh container, fine.
		return nil
	}
	// Bounded retry on transient failure (network blip, executor 5xx,
	// 502 from api-server). 4xx (incl. 403 token mismatch, 410 window
	// closed) won't recover — bail immediately and let the consecutive-
	// 403s counter handle the poisoned-container case via the user's
	// next call.
	status, err := m.pushRestore(c, accessToken, tarBytes)
	if err != nil && (status == 0 || status >= 500) {
		log.Printf("orchestrator: pushRestore transient failure for session %s: %v — retrying once", accessToken, err)
		time.Sleep(restorePushRetryDelay)
		_, err = m.pushRestore(c, accessToken, tarBytes)
	}
	if err != nil {
		return fmt.Errorf("push restore: %w", err)
	}
	return nil
}

func (m *Manager) CleanupSession(accessToken string) *Container {
	m.mu.Lock()
	c, ok := m.sessions[accessToken]
	if ok {
		delete(m.sessions, accessToken)
	}
	delete(m.assignLocks, accessToken)
	m.mu.Unlock()
	if !ok {
		return nil
	}
	c.Status = "deleting"
	log.Printf("orchestrator: cleaning up %s for session %s", c.Name, accessToken)
	go m.deleteContainer(c.ID)
	return c
}

// evictAndSnapshot snapshots the container's workspace (if a Code
// Execution Encryption Key and bearer were cached) and PUTs the
// plaintext tar to buckets, which encrypts it under the cached key.
// Then deletes the container. Called by the idle-eviction loop.
// Snapshot failures still destroy the container — losing state is
// bad, but leaving stale containers around is worse and the user can
// always start fresh.
func (m *Manager) evictAndSnapshot(accessToken string, c *Container) {
	switch {
	case c.CodeExecutionEncryptionKey == "":
		log.Printf("orchestrator: no code execution encryption key cached for session %s — skipping snapshot", accessToken)
	case c.Bearer == "":
		log.Printf("orchestrator: no api_key bearer cached for session %s — skipping snapshot", accessToken)
	default:
		ctx := context.Background()
		tarBytes, err := m.fetchSnapshotFromContainer(c, accessToken)
		if err != nil {
			log.Printf("orchestrator: snapshot failed for session %s on %s: %v", accessToken, c.Name, err)
		} else {
			// One retry on transient PUT failure: a single buckets blip
			// shouldn't cost a user their workspace. Beyond that we accept
			// the loss and the user starts fresh on next chat open.
			putErr := m.putSnapshotTar(ctx, c.Bearer, accessToken, c.CodeExecutionEncryptionKey, tarBytes)
			if putErr != nil {
				log.Printf("orchestrator: PUT snapshot failed for session %s: %v — retrying once", accessToken, putErr)
				time.Sleep(snapshotPutRetryDelay)
				putErr = m.putSnapshotTar(ctx, c.Bearer, accessToken, c.CodeExecutionEncryptionKey, tarBytes)
			}
			if putErr != nil {
				log.Printf("orchestrator: PUT snapshot failed for session %s after retry: %v", accessToken, putErr)
			} else {
				log.Printf("orchestrator: snapshotted session %s (container %s) to buckets", accessToken, c.Name)
			}
		}
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
		accessToken string
		c           *Container
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
		delete(m.sessions, t.accessToken)
	}
	m.mu.Unlock()

	for _, t := range targets {
		log.Printf("orchestrator: idle-evicting session %s on %s (idle ~%v)",
			t.accessToken, t.c.Name, m.cfg.IdleTimeout)
		m.evictAndSnapshot(t.accessToken, t.c)
		m.mu.Lock()
		delete(m.assignLocks, t.accessToken)
		m.mu.Unlock()
	}
}

// ---------------------------------------------------------------------------
// Health checking
// ---------------------------------------------------------------------------

func (m *Manager) checkContainerHealth(c *Container) bool {
	if c.httpClient == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", "https://"+c.Domain+"/health", nil)
	if err != nil {
		return false
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode == http.StatusOK
}

// StartHealthCheckLoop launches the background goroutine that probes
// every warm container's /health on a fixed cadence. Containers that
// fail MaxHealthFailures consecutive checks are evicted + destroyed; the
// pool manager replenishes on its next tick.
func (m *Manager) StartHealthCheckLoop() {
	go m.healthCheckLoop()
}

func (m *Manager) healthCheckLoop() {
	for !m.shuttingDown {
		time.Sleep(m.cfg.HealthCheckInterval)
		if m.shuttingDown {
			return
		}
		m.scanWarmHealth()
	}
}

// scanWarmHealth probes /health on each warm container once. Successes
// reset HealthFailures; failures bump it. A container that hits
// MaxHealthFailures is removed from the warm pool and destroyed. We
// snapshot the warm pool under m.mu, then run the (slow) network
// probes lock-free. A container that's been pulled by GetOrAssign in
// the meantime is no longer in m.warmPool when we go to evict, so the
// final removal step is a no-op for it — that's fine, an in-flight
// request will surface the failure on its own (proxy 502 or eviction).
func (m *Manager) scanWarmHealth() {
	m.mu.Lock()
	targets := append([]*Container(nil), m.warmPool...)
	m.mu.Unlock()

	for _, c := range targets {
		if m.checkContainerHealth(c) {
			m.mu.Lock()
			c.HealthFailures = 0
			m.mu.Unlock()
			continue
		}
		m.mu.Lock()
		c.HealthFailures++
		n := c.HealthFailures
		m.mu.Unlock()
		if n < m.cfg.MaxHealthFailures {
			log.Printf("orchestrator: warm container %s /health failed (%d/%d)", c.Name, n, m.cfg.MaxHealthFailures)
			continue
		}

		m.mu.Lock()
		evicted := false
		for i, x := range m.warmPool {
			if x == c {
				m.warmPool = append(m.warmPool[:i], m.warmPool[i+1:]...)
				evicted = true
				break
			}
		}
		if evicted {
			c.Status = "failed"
			m.failed = append(m.failed, c)
			m.failCount++
			for len(m.failed) > 10 {
				m.failed = m.failed[1:]
			}
		}
		m.mu.Unlock()

		if evicted {
			log.Printf("orchestrator: warm container %s exceeded health failure threshold (%d) — destroying",
				c.Name, m.cfg.MaxHealthFailures)
			go m.deleteContainer(c.ID)
		}
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

func (m *Manager) proxy(ctx context.Context, c *Container, accessToken, path string, body []byte) (int, []byte, error) {
	if c.httpClient == nil {
		return 0, nil, fmt.Errorf("no http client for container %s", c.Name)
	}
	url := "https://" + c.Domain + path
	callCtx, cancel := context.WithTimeout(ctx, toolCallTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(callCtx, "POST", url, bytes.NewReader(body))
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
	data, _ := io.ReadAll(resp.Body)
	// Record activity on every reachable proxy hop. The eviction loop
	// uses this to compute idleness, so we want it bumped even on
	// non-2xx responses (the user is still interacting with us).
	c.LastActivity = time.Now()
	m.recordContainerStatus(accessToken, c, resp.StatusCode)
	return resp.StatusCode, data, nil
}

// recordContainerStatus updates the consecutive-403s counter on c after a
// container-bound call. 2xx resets to 0; 403 increments and triggers
// CleanupSession at threshold. Other status codes (incl. 401, which
// would be an orchestrator bug not a poisoned container) are ignored.
//
// Invoked by every path that talks to the executor: proxy (exec/read/
// write), pushRestore, fetchSnapshotFromContainer.
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
		log.Printf("orchestrator: container %s rejected access token for session %s twice — destroying", c.Name, accessToken)
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
		return map[string]any{"error": fmt.Sprintf("proxy returned status %d", status), "raw": string(raw)}
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

	rec := func(c *Container, accessToken string) map[string]any {
		d := map[string]any{
			"id":     c.ID,
			"name":   c.Name,
			"status": c.Status,
			"uptime": int(now.Sub(c.CreatedAt).Seconds()),
		}
		if accessToken != "" {
			d["code_execution_access_token"] = accessToken
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
