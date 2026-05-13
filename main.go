// Orchestrator entry point.
//
// Thin HTTP server that routes:
//
//	POST /mcp     → MCP handler (primary tool interface)
//	GET  /metrics → Prometheus scrape (aggregate counters/gauges only)
//
// On SIGINT/SIGTERM the orchestrator snapshots every active session to
// buckets & deletes every container it owns
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Snapshot cap covers /workspace tmpfs (512 MB) → ~683 MB after base64.
const (
	maxMCPRequestBody   = 1 << 20  // 1 MB
	maxControlplaneBody = 1 << 20  // 1 MB
	maxExecutorBody     = 16 << 20 // 16 MB
	maxSnapshotBody     = 1 << 30  // 1 GB
)

// readLimited errors if r exceeds max, instead of silently truncating.
func readLimited(r io.Reader, max int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return data, err
	}
	if int64(len(data)) > max {
		return nil, fmt.Errorf("response exceeds %d bytes", max)
	}
	return data, nil
}

func envStr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func main() {
	adminAPIKey := os.Getenv("ADMIN_API_KEY")
	if adminAPIKey == "" {
		log.Fatal("ADMIN_API_KEY is required")
	}

	cfg := ManagerConfig{
		AdminAPIKey:            adminAPIKey,
		ControlPlaneURL:        envStr("CONTROL_PLANE_URL", "https://api.tinfoil.sh"),
		BucketsBase:            envStr("BUCKETS_BASE", "https://buckets.tinfoil.sh"),
		PoolSize:               envInt("POOL_SIZE", 3),
		MaxContainers:          envInt("MAX_CONTAINERS", 10),
		PollInterval:           time.Duration(envInt("POLL_INTERVAL", 2)) * time.Second,
		IdleTimeout:            time.Duration(envInt("IDLE_TIMEOUT", 60)) * time.Second,
		EvictionPoll:           time.Duration(envInt("EVICTION_POLL", 30)) * time.Second,
		WarmPoolWaitTimeout:    time.Duration(envInt("WARM_POOL_WAIT_TIMEOUT", 10)) * time.Second,
		HealthCheckInterval:    time.Duration(envInt("HEALTH_CHECK_INTERVAL", 15)) * time.Second,
		MaxHealthFailures:      envInt("MAX_HEALTH_FAILURES", 3),
		MaxConcurrentSnapshots: envInt("MAX_CONCURRENT_SNAPSHOTS", 4),
		ShutdownDeadline:       time.Duration(envInt("SHUTDOWN_DEADLINE", 25)) * time.Second,
		// Execution Environment
		EnvironmentRepo: envStr("ENVIRONMENT_REPO", "tinfoilsh/code-execution-environment"),
		EnvironmentTag:  envStr("ENVIRONMENT_TAG", "v0.0.11"),
	}

	port := envInt("PORT", 7070)

	log.Printf("orchestrator: pool=%d max=%d env=%s:%s",
		cfg.PoolSize, cfg.MaxContainers, cfg.EnvironmentRepo, cfg.EnvironmentTag)
	mgr := NewManager(cfg)
	registerPoolGauges(mgr)
	mgr.StartPoolManager()
	mgr.StartEvictionLoop()
	mgr.StartHealthCheckLoop()

	mux := http.NewServeMux()
	srv := &http.Server{Addr: ":" + strconv.Itoa(port), Handler: mux}

	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/mcp", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxMCPRequestBody)
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
			return
		}
		status, resp := HandleMCPRequest(r.Context(), mgr, r.Header, req)
		if resp == nil {
			w.WriteHeader(status)
			return
		}
		writeJSON(w, status, resp)
	})

	// On SIGINT/SIGTERM, stop accepting new requests, then run Finish()
	// to snapshot active sessions and bulk-delete containers.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("orchestrator: caught signal, finalizing...")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
		mgr.Finish()
	}()

	log.Printf("orchestrator listening on :%d", port)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
