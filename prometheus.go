package main

// Prometheus metrics exposed at /metrics.
// All metrics are aggregate — no per-session, per-user, or per-container
// identity is recorded.

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	containersCreated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "orchestrator_containers_created_total",
		Help: "Containers successfully created on the controlplane.",
	})
	containersDeleted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "orchestrator_containers_deleted_total",
		Help: "Container delete requests sent to the controlplane (any reason).",
	})
	controlplaneErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "orchestrator_controlplane_errors_total",
		Help: "Transport errors and 5xx responses from the controlplane API.",
	})
	attestationFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "orchestrator_attestation_failures_total",
		Help: "Containers that failed attestation during inflight→ready promotion.",
	})
	snapshots = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "orchestrator_snapshots_total",
		Help: "Snapshot attempts at eviction time, by result. 'skipped' = no keys cached.",
	}, []string{"result"})
	restores = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "orchestrator_restores_total",
		Help: "Restore-on-assign attempts, by result. 'empty' = no snapshot in bucket.",
	}, []string{"result"})
	healthFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "orchestrator_health_failures_total",
		Help: "Containers evicted after exceeding the consecutive /health failure threshold.",
	}, []string{"kind"})
)

// registerPoolGauges binds the in-memory pool sizes to gauges that
// Prometheus reads on each scrape. Holds m.mu briefly per scrape.
func registerPoolGauges(m *Manager) {
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "orchestrator_warm_pool_size",
		Help: "Containers ready to be assigned to a session.",
	}, func() float64 {
		m.mu.Lock()
		defer m.mu.Unlock()
		return float64(len(m.warmPool))
	})
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "orchestrator_inflight_size",
		Help: "Containers being deployed (not yet ready).",
	}, func() float64 {
		m.mu.Lock()
		defer m.mu.Unlock()
		return float64(len(m.inflight))
	})
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "orchestrator_sessions_active",
		Help: "Containers currently assigned to a session.",
	}, func() float64 {
		m.mu.Lock()
		defer m.mu.Unlock()
		return float64(len(m.sessions))
	})
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "orchestrator_pool_target",
		Help: "Configured warm pool target (POOL_SIZE).",
	}, func() float64 { return float64(m.cfg.PoolSize) })
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "orchestrator_max_containers",
		Help: "Configured hard cap on concurrent containers (MAX_CONTAINERS).",
	}, func() float64 { return float64(m.cfg.MaxContainers) })
}
