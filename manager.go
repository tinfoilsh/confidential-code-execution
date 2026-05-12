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
	AccessToken                string // X-Code-Execution-Access-Token; same as Manager.sessions map key
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
	// Caps how many snapshot/restore tar buffers can live in memory at
	// once.
	MaxConcurrentSnapshots int
	// Caps Finish's session snapshot phase before bulk-delete.
	// TODO: tune this correctly
	ShutdownDeadline time.Duration
}

type Manager struct {
	cfg ManagerConfig

	mu          sync.Mutex
	cond        *sync.Cond
	warmPool    []*Container
	inflight    []*Container
	sessions    map[string]*Container
	assignLocks map[string]*sync.Mutex // see lockSession
	finishOnce  sync.Once
	done        chan struct{} // closed by Finish to signal shutdown

	// Bearers that recently passed validate-key
	authCacheMu sync.Mutex
	authCache   map[string]time.Time

	// Bounds how many snapshot/restore tar buffers can be in memory concurrently.
	snapshotSem chan struct{}

	cp      *Controlplane
	buckets *Buckets
}

func NewManager(cfg ManagerConfig) *Manager {
	m := &Manager{
		cfg:         cfg,
		cp:          NewControlplane(cfg.ControlPlaneURL, cfg.AdminAPIKey),
		buckets:     NewBuckets(cfg.BucketsBase),
		sessions:    map[string]*Container{},
		assignLocks: map[string]*sync.Mutex{},
		done:        make(chan struct{}),
		authCache:   map[string]time.Time{},
		snapshotSem: make(chan struct{}, cfg.MaxConcurrentSnapshots),
	}
	m.cond = sync.NewCond(&m.mu)
	return m
}

// lockSession serializes the read-or-restore-then-assign critical
// section across concurrent GetOrAssign calls for the same lockSession
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
	target := min(len(m.sessions)+m.cfg.PoolSize, m.cfg.MaxContainers)
	needed := target - total
	m.mu.Unlock()

	var created []*Container
	for range needed {
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

		// Exponential backoff capped at 16× and 30s.
		delay := min(m.cfg.PollInterval<<min(consecutiveFailures, 4), 30*time.Second)
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
// calls for the same session serialize on a per-session mutex.
// Re-probes /health on the popped warm container — catches deaths
// between health-loop ticks.
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

	// If we never end up registering a session, drop the assignLocks so it doesn't grow the map
	assigned := false
	defer func() {
		if assigned {
			return
		}
		m.mu.Lock()
		if _, ok := m.sessions[accessToken]; !ok {
			delete(m.assignLocks, accessToken)
		}
		m.mu.Unlock()
	}()

	// Re-check after acquiring sl: another goroutine may have assigned
	// while we were waiting.
	m.mu.Lock()
	if c, ok := m.sessions[accessToken]; ok {
		m.mu.Unlock()
		assigned = true
		return c, ""
	}
	m.mu.Unlock()

	// Reserve an in-memory tar slot upfront (only if we'll actually
	// restore). At capacity, refuse before popping a warm container so the
	// caller can retry without us churning state.
	willRestore := codeExecutionEncryptionKey != "" && bearer != ""
	if willRestore {
		select {
		case m.snapshotSem <- struct{}{}:
		default:
			restores.WithLabelValues("failure").Inc()
			return nil, "server at capacity, please retry"
		}
		defer func() { <-m.snapshotSem }()
	}

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

	// Restore in two phases. The container's state stays exactly as it
	// was popped from warm — no fields written until restore succeeds —
	// so a fetch failure recycles it untouched while push  destroys it.
	if willRestore {
		tarBytes, err := m.buckets.fetch(ctx, bearer, accessToken, codeExecutionEncryptionKey)
		if err != nil {
			restores.WithLabelValues("failure").Inc()
			m.mu.Lock()
			m.warmPool = append(m.warmPool, c)
			m.cond.Broadcast()
			m.mu.Unlock()
			return nil, fmt.Sprintf("fetch snapshot: %v", err)
		}
		if tarBytes == nil {
			restores.WithLabelValues("empty").Inc()
		} else {
			if err := m.pushRestore(ctx, c, tarBytes); err != nil {
				restores.WithLabelValues("failure").Inc()
				c.setStatus("failed")
				go m.cp.deleteContainer(c.ID)
				return nil, fmt.Sprintf("push restore: %v", err)
			}
			restores.WithLabelValues("success").Inc()
		}
	}

	// Commit assigned state in one shot, only after restore has succeeded or skippped.
	// Until this point the container looked identical to a warm-pool entry
	now := time.Now()
	c.mu.Lock()
	c.Status = "assigned"
	c.AssignedAt = now
	c.LastActivity = now
	c.CodeExecutionEncryptionKey = codeExecutionEncryptionKey
	c.Bearer = bearer
	c.AccessToken = accessToken
	c.mu.Unlock()
	m.mu.Lock()
	m.sessions[accessToken] = c
	m.mu.Unlock()
	assigned = true

	return c, ""
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
// cached) then deletes the container. Snapshot failures still destroy.
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
		// Unbounded wait on evictIdle, bounded wait on Finish
		select {
		case m.snapshotSem <- struct{}{}:
		case <-ctx.Done():
			snapshots.WithLabelValues("skipped").Inc()
			c.setStatus("deleting")
			m.cp.deleteContainer(c.ID)
			return
		}
		tarBytes, err := m.fetchSnapshotFromContainer(ctx, c)
		if err != nil {
			snapshots.WithLabelValues("failure").Inc()
		} else if err := m.buckets.put(ctx, bearer, accessToken, key, tarBytes); err != nil {
			snapshots.WithLabelValues("failure").Inc()
		} else {
			snapshots.WithLabelValues("success").Inc()
		}
		<-m.snapshotSem
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
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("orchestrator: eviction loop error: %v", r)
					}
				}()
				m.evictIdleSessions()
			}()
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
	m.mu.Unlock()

	for _, t := range targets {
		m.evictSessionAsync(t.accessToken, t.c)
	}
}

// evictSessionAsync spawns a goroutine that evicts (accessToken → c).
// It holds the per-session assign lock through evictAndSnapshot so a
// concurrent GetOrAssign queues behind u.
// Re-verifies the session under m.mu so Finish / CleanupSession / a racing
// health-eviction goroutine can't double-evict.
func (m *Manager) evictSessionAsync(accessToken string, c *Container) {
	sl := m.lockSession(accessToken)
	go func() {
		sl.Lock()
		defer sl.Unlock()
		m.mu.Lock()
		cur, ok := m.sessions[accessToken]
		if !ok || cur != c {
			m.mu.Unlock()
			return
		}
		delete(m.sessions, accessToken)
		m.mu.Unlock()
		m.evictAndSnapshot(context.Background(), accessToken, c)
		m.mu.Lock()
		delete(m.assignLocks, accessToken)
		m.mu.Unlock()
	}()
}

// ---------------------------------------------------------------------------
// Health checking
// ---------------------------------------------------------------------------

func (m *Manager) StartHealthCheckLoop() {
	go m.healthCheckLoop()
}

func (m *Manager) healthCheckLoop() {
	for {
		select {
		case <-m.done:
			return
		case <-time.After(m.cfg.HealthCheckInterval):
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("orchestrator: health check loop error: %v", r)
					}
				}()
				m.scanWarmHealth()
				m.scanSessionHealth()
			}()
		}
	}
}

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

// Async so a slow snapshot doesn't block the next health tick.
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
		m.evictSessionAsync(t.accessToken, t.c)
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

// Finish snapshots every active session in parallel then bulk-deletes any remaining containers.
// Sessions whose snapshot doesn't finish within ShutdownDeadline lose state.
func (m *Manager) Finish() {
	m.finishOnce.Do(func() {
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
	})
}
