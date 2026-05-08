package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

var snapshotPutRetryDelay = 1 * time.Second
var restorePushRetryDelay = 1 * time.Second
var toolCallTimeout = 35 * time.Second

type Container struct {
	// Not mutated
	ID        string
	Name      string
	Domain    string
	CreatedAt time.Time
	// httpClient set before publishing to warmPool — no concurrent reader yet.
	httpClient *http.Client

	// mu guards every field below.
	// Lock ordering: Manager.mu before Container.mu when both are held.
	mu                         sync.Mutex
	Status                     string
	AssignedAt                 time.Time
	LastActivity               time.Time
	Bearer                     string // api_key from the request's Authorization header
	CodeExecutionEncryptionKey string // X-Code-Execution-Encryption-Key from request
	Consecutive403s            int
	HealthFailures             int
}

func (c *Container) setStatus(s string) {
	c.mu.Lock()
	c.Status = s
	c.mu.Unlock()
}

func (c *Container) bumpActivity() {
	c.mu.Lock()
	c.LastActivity = time.Now()
	c.mu.Unlock()
}

type ManagerConfig struct {
	AdminAPIKey     string
	ControlPlaneURL string // CONTROL_PLANE_URL
	BucketsBase     string // BUCKETS_BASE
	PoolSize        int
	MaxContainers   int
	PollInterval    time.Duration
	EnvironmentRepo string
	EnvironmentTag  string
	// Session lifecycle
	IdleTimeout         time.Duration
	EvictionPoll        time.Duration
	WarmPoolWaitTimeout time.Duration
	HealthCheckInterval time.Duration
	MaxHealthFailures   int
	// Caps Finish's session snapshot phase before bulk-delete.
	// TODO: tune this correctly
	ShutdownDeadline time.Duration
	// Local dev only. Skip attestation & don't require key
	DevSkipAttestation bool
	DevBypassAuth      bool
}

type Manager struct {
	cfg ManagerConfig

	mu          sync.Mutex
	cond        *sync.Cond
	warmPool    []*Container
	inflight    []*Container
	sessions    map[string]*Container
	assignLocks map[string]*sync.Mutex // see lockSession
	done        chan struct{}          // closed by Finish to signal shutdown

	cp      *Controlplane
	buckets *Buckets
}

func NewManager(cfg ManagerConfig) *Manager {
	if cfg.ControlPlaneURL == "" {
		cfg.ControlPlaneURL = "https://api.tinfoil.sh"
	}
	if cfg.BucketsBase == "" {
		cfg.BucketsBase = "https://buckets.tinfoil.sh"
	}
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
	if cfg.ShutdownDeadline == 0 {
		cfg.ShutdownDeadline = 25 * time.Second
	}
	m := &Manager{
		cfg:         cfg,
		cp:          NewControlplane(cfg.ControlPlaneURL, cfg.AdminAPIKey),
		buckets:     NewBuckets(cfg.BucketsBase),
		sessions:    map[string]*Container{},
		assignLocks: map[string]*sync.Mutex{},
		done:        make(chan struct{}),
	}
	m.cond = sync.NewCond(&m.mu)
	return m
}

// lockSession serializes the read-or-restore-then-assign critical
// section across concurrent GetOrAssign calls for the same session, so
// two tabs from the same chat don't both pull a fresh container and
// double-restore. Locks are kept until CleanupSession.
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
		c, err := m.cp.createContainer(m.cfg.EnvironmentRepo, m.cfg.EnvironmentTag)
		if err != nil {
			break
		}
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
		s := m.cp.pollContainer(c.ID)
		switch s {
		case "ready":
			cli, err := m.buildProxyClient(c)
			if err != nil {
				attestationFailures.Inc()
				failed = append(failed, c)
				continue
			}
			// httpClient set before publishing to warmPool — no concurrent reader yet.
			c.httpClient = cli
			c.setStatus("ready")
			ready = append(ready, c)
		case "failed":
			failed = append(failed, c)
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
	}
	if len(ready) > 0 {
		m.cond.Broadcast()
	}
	m.mu.Unlock()
	for _, c := range failed {
		c.setStatus("failed")
	}

	for _, c := range failed {
		go m.cp.deleteContainer(c.ID)
	}
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
	for {
		var created []*Container
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("orchestrator: pool manager error: %v", r)
				}
			}()
			created = m.replenishPool()
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
		select {
		case <-m.done:
			return
		case <-time.After(delay):
		}
	}
}

// ---------------------------------------------------------------------------
// Session management
// ---------------------------------------------------------------------------

// GetOrAssign returns the session's existing container or pops one from
// the warm pool, runs restore-on-assign, and registers it. Concurrent
// calls for the same session serialize on a per-session mutex so they
// share one container. Pre-assign /health filters warm containers that
// died between the periodic check and now.
func (m *Manager) GetOrAssign(ctx context.Context, accessToken string, isConnected func() bool) (*Container, string) {
	codeExecutionEncryptionKey := sessionCodeExecutionEncryptionKey(ctx)
	bearer := sessionBearer(ctx)

	// Fast path: existing session — no per-session lock needed since
	// we're just refreshing keys on a container that's already in the map.
	m.mu.Lock()
	if c, ok := m.sessions[accessToken]; ok {
		m.mu.Unlock()
		c.mu.Lock()
		if codeExecutionEncryptionKey != "" {
			c.CodeExecutionEncryptionKey = codeExecutionEncryptionKey
		}
		if bearer != "" {
			c.Bearer = bearer
		}
		c.mu.Unlock()
		return c, ""
	}
	// Capacity check before lockSession so a flood of unique tokens
	// can't grow assignLocks past MaxContainers.
	if len(m.sessions) >= m.cfg.MaxContainers {
		m.mu.Unlock()
		return nil, fmt.Sprintf("at capacity (%d sessions)", m.cfg.MaxContainers)
	}
	m.mu.Unlock()

	sl := m.lockSession(accessToken)
	sl.Lock()
	defer sl.Unlock()

	// Re-check after acquiring sl: another goroutine may have assigned
	// while we were waiting.
	m.mu.Lock()
	if c, ok := m.sessions[accessToken]; ok {
		m.mu.Unlock()
		return c, ""
	}
	m.mu.Unlock()
	var c *Container

	deadline := time.Now().Add(m.cfg.WarmPoolWaitTimeout)
	for {
		m.mu.Lock()
		for len(m.warmPool) == 0 {
			select {
			case <-m.done:
				m.mu.Unlock()
				return nil, "shutting down"
			default:
			}
			if time.Now().After(deadline) {
				m.mu.Unlock()
				return nil, fmt.Sprintf("no containers available (timed out after %v)", m.cfg.WarmPoolWaitTimeout)
			}
			if isConnected != nil && !isConnected() {
				m.mu.Unlock()
				return nil, "client disconnected"
			}
			// wake every 2s to re-check connection / deadline
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
		candidate.setStatus("failed")
		go m.cp.deleteContainer(candidate.ID)
	}

	now := time.Now()
	c.mu.Lock()
	c.Status = "assigning"
	c.AssignedAt = now
	c.LastActivity = now
	if codeExecutionEncryptionKey != "" {
		c.CodeExecutionEncryptionKey = codeExecutionEncryptionKey
	}
	if bearer != "" {
		c.Bearer = bearer
	}
	c.mu.Unlock()

	// Restore failures fall through to a fresh empty workspace — better
	// than refusing to assign and breaking the user's chat.
	if codeExecutionEncryptionKey != "" && bearer != "" {
		_ = m.restoreInto(ctx, bearer, accessToken, c, codeExecutionEncryptionKey)
	}

	// Mark assigned only AFTER restore so the eviction loop can't trip
	// on a half-bootstrapped container.
	c.setStatus("assigned")
	m.mu.Lock()
	m.sessions[accessToken] = c
	m.mu.Unlock()

	return c, ""
}

// restoreInto fetches the snapshot for accessToken from buckets and
// pushes the plaintext tar into the container's /restore. Empty bucket
// or unreadable-with-this-key both surface as a fresh workspace.
func (m *Manager) restoreInto(ctx context.Context, bearer, accessToken string, c *Container, codeExecutionEncryptionKeyB64 string) error {
	tarBytes, err := m.buckets.fetch(ctx, bearer, accessToken, codeExecutionEncryptionKeyB64)
	if err != nil {
		restores.WithLabelValues("failure").Inc()
		return fmt.Errorf("fetch snapshot: %w", err)
	}
	if tarBytes == nil {
		restores.WithLabelValues("empty").Inc()
		return nil
	}
	// One retry on transient (5xx, network). 4xx (incl. 403 token
	// mismatch, 410 window closed) won't recover — bail.
	status, err := m.pushRestore(c, accessToken, tarBytes)
	if err != nil && (status == 0 || status >= 500) {
		time.Sleep(restorePushRetryDelay)
		_, err = m.pushRestore(c, accessToken, tarBytes)
	}
	if err != nil {
		restores.WithLabelValues("failure").Inc()
		return fmt.Errorf("push restore: %w", err)
	}
	restores.WithLabelValues("success").Inc()
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
	c.setStatus("deleting")
	go m.cp.deleteContainer(c.ID)
	return c
}

// evictAndSnapshot snapshots the workspace to buckets (if keys are
// cached) then deletes the container. Snapshot failures still destroy:
// losing state is bad, leaving stale containers is worse.
// ctx bounds the snapshot phase — Finish passes a deadlined ctx so
// in-flight HTTP gets cancelled when the platform grace period nears.
func (m *Manager) evictAndSnapshot(ctx context.Context, accessToken string, c *Container) {
	c.mu.Lock()
	bearer, key := c.Bearer, c.CodeExecutionEncryptionKey
	c.mu.Unlock()

	switch {
	case key == "", bearer == "":
		snapshots.WithLabelValues("skipped").Inc()
	default:
		tarBytes, err := m.fetchSnapshotFromContainer(ctx, c, accessToken)
		if err != nil {
			snapshots.WithLabelValues("failure").Inc()
		} else {
			// One retry on transient PUT failure: a single buckets blip
			// shouldn't cost a user their workspace.
			putErr := m.buckets.put(ctx, bearer, accessToken, key, tarBytes)
			if putErr != nil {
				time.Sleep(snapshotPutRetryDelay)
				putErr = m.buckets.put(ctx, bearer, accessToken, key, tarBytes)
			}
			if putErr != nil {
				snapshots.WithLabelValues("failure").Inc()
			} else {
				snapshots.WithLabelValues("success").Inc()
			}
		}
	}
	c.setStatus("deleting")
	m.cp.deleteContainer(c.ID)
}

func (m *Manager) StartEvictionLoop() {
	go m.evictionLoop()
}

func (m *Manager) evictionLoop() {
	for {
		select {
		case <-m.done:
			return
		case <-time.After(m.cfg.EvictionPoll):
			m.evictIdleSessions()
		}
	}
}

func (m *Manager) evictIdleSessions() {
	type target struct {
		accessToken string
		c           *Container
	}
	now := time.Now()
	var targets []target

	m.mu.Lock()
	for sid, c := range m.sessions {
		c.mu.Lock()
		idle := now.Sub(c.LastActivity) >= m.cfg.IdleTimeout
		c.mu.Unlock()
		if idle {
			targets = append(targets, target{sid, c})
		}
	}
	// Drop session entries up front but keep the assign lock so a
	// concurrent GetOrAssign queues behind us cleanly.
	for _, t := range targets {
		delete(m.sessions, t.accessToken)
	}
	m.mu.Unlock()

	for _, t := range targets {
		m.evictAndSnapshot(context.Background(), t.accessToken, t.c)
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

func (m *Manager) StartHealthCheckLoop() {
	go m.healthCheckLoop()
}

func (m *Manager) healthCheckLoop() {
	for {
		select {
		case <-m.done:
			return
		case <-time.After(m.cfg.HealthCheckInterval):
			m.scanWarmHealth()
			m.scanSessionHealth()
		}
	}
}

// label ("warm" / "session")
func (m *Manager) shouldEvictForHealth(c *Container, label string) bool {
	if m.checkContainerHealth(c) {
		c.mu.Lock()
		c.HealthFailures = 0
		c.mu.Unlock()
		return false
	}
	c.mu.Lock()
	c.HealthFailures++
	n := c.HealthFailures
	c.mu.Unlock()
	if n < m.cfg.MaxHealthFailures {
		return false
	}
	healthFailures.WithLabelValues(label).Inc()
	return true
}

func (m *Manager) scanWarmHealth() {
	m.mu.Lock()
	targets := append([]*Container(nil), m.warmPool...)
	m.mu.Unlock()

	for _, c := range targets {
		if !m.shouldEvictForHealth(c, "warm") {
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
		m.mu.Unlock()
		if evicted {
			c.setStatus("failed")
			go m.cp.deleteContainer(c.ID)
		}
	}
}

// scanSessionHealth routes failed session containers through
// evictAndSnapshot — workspace state may survive when the api-server is
// the part that's wedged. Async so a slow snapshot doesn't block the
// next health tick.
func (m *Manager) scanSessionHealth() {
	type target struct {
		accessToken string
		c           *Container
	}
	m.mu.Lock()
	targets := make([]target, 0, len(m.sessions))
	for sid, c := range m.sessions {
		targets = append(targets, target{sid, c})
	}
	m.mu.Unlock()

	for _, t := range targets {
		if !m.shouldEvictForHealth(t.c, "session") {
			continue
		}
		m.mu.Lock()
		// Drop only if it's still us — racing eviction or assign-reuse
		// could have already moved/replaced this entry.
		cur, ok := m.sessions[t.accessToken]
		if !ok || cur != t.c {
			m.mu.Unlock()
			continue
		}
		delete(m.sessions, t.accessToken)
		m.mu.Unlock()

		go func(accessToken string, c *Container) {
			m.evictAndSnapshot(context.Background(), accessToken, c)
			m.mu.Lock()
			delete(m.assignLocks, accessToken)
			m.mu.Unlock()
		}(t.accessToken, t.c)
	}
}

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------

func (m *Manager) CleanupAll() {
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
	m.mu.Unlock()

	for _, c := range all {
		if !m.cp.verifyContainerName(c) {
			continue
		}
		m.cp.deleteContainer(c.ID)
	}
}

// Finish snapshots every active session in parallel (mirroring idle
// eviction) then bulk-deletes any remaining containers. Sessions whose
// snapshot doesn't finish within ShutdownDeadline fall through to
// CleanupAll's plain delete — their state is lost.
func (m *Manager) Finish() {
	close(m.done)
	// Wake any GetOrAssign waiters parked on cond so they exit promptly.
	m.mu.Lock()
	m.cond.Broadcast()
	m.mu.Unlock()

	type target struct {
		accessToken string
		c           *Container
	}
	var targets []target
	m.mu.Lock()
	for sid, c := range m.sessions {
		targets = append(targets, target{sid, c})
	}
	// Drop session entries up front so CleanupAll doesn't double-delete
	// the container we're about to evictAndSnapshot.
	m.sessions = map[string]*Container{}
	m.mu.Unlock()

	if len(targets) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), m.cfg.ShutdownDeadline)
		defer cancel()
		var wg sync.WaitGroup
		for _, t := range targets {
			wg.Add(1)
			go func(t target) {
				defer wg.Done()
				m.evictAndSnapshot(ctx, t.accessToken, t.c)
			}(t)
		}
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-ctx.Done():
			log.Printf("orchestrator: shutdown deadline (%v) hit, %d session snapshot(s) may have been lost", m.cfg.ShutdownDeadline, len(targets))
		}
	}

	m.CleanupAll()
}
