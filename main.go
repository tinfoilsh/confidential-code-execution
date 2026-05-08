// Orchestrator entry point.
//
// Thin HTTP server that routes:
//
//	POST /mcp        → MCP handler (primary tool interface)
//	GET  /health     → health check
//	GET  /metrics    → detailed metrics for viz.py
//	POST /cleanup    → release a single session
//	POST /delete-all → delete all containers
//	POST /finish     → delete all + shutdown
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

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

func envBool(key string, def bool) bool {
	if v, ok := os.LookupEnv(key); ok {
		return v == "true" || v == "1" || v == "True" || v == "TRUE"
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
		AdminAPIKey:         adminAPIKey,
		PoolSize:            envInt("POOL_SIZE", 3),
		MaxContainers:       envInt("MAX_CONTAINERS", 10),
		PollInterval:        time.Duration(envInt("POLL_INTERVAL", 2)) * time.Second,
		ConfigRepo:          envStr("CONFIG_REPO", "tinfoilsh/code-execution-environment"),
		ConfigTag:           envStr("CONFIG_TAG", "v0.0.9"),
		VerifyAttestation:   envBool("VERIFY_ATTESTATION", false),
		DevBypassAuth:       envBool("DEV_BYPASS_AUTH", false),
		WarmPoolWaitTimeout: time.Duration(envInt("WARM_POOL_WAIT_TIMEOUT", 10)) * time.Second,
		HealthCheckInterval: time.Duration(envInt("HEALTH_CHECK_INTERVAL", 15)) * time.Second,
		MaxHealthFailures:   envInt("MAX_HEALTH_FAILURES", 3),
	}

	// Snapshot storage lives at tinfoil-buckets. Default points at prod;
	// override for local dev or staging via BUCKETS_BASE.
	bucketsBase = envStr("BUCKETS_BASE", bucketsBase)
	port := envInt("PORT", 7070)

	log.Printf("orchestrator: pool_size=%d max_containers=%d poll_interval=%v verify_attestation=%v",
		cfg.PoolSize, cfg.MaxContainers, cfg.PollInterval, cfg.VerifyAttestation)
	log.Printf("orchestrator: repo=%s tag=%s", cfg.ConfigRepo, cfg.ConfigTag)
	mgr := NewManager(cfg)
	mgr.StartPoolManager()
	mgr.StartEvictionLoop()
	mgr.StartHealthCheckLoop()

	mux := http.NewServeMux()
	srv := &http.Server{Addr: ":" + strconv.Itoa(port), Handler: mux}

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, mgr.HealthInfo())
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, mgr.MetricsInfo())
	})
	mux.HandleFunc("/cleanup", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			AccessToken string `json:"codeExecutionAccessToken"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		if body.AccessToken == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "codeExecutionAccessToken is required"})
			return
		}
		c := mgr.CleanupSession(body.AccessToken)
		if c == nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "no session found for " + body.AccessToken})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "cleaned up", "container": c.Name})
	})
	mux.HandleFunc("/delete-all", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, mgr.CleanupAll())
	})
	mux.HandleFunc("/finish", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, mgr.Finish())
		go func() {
			time.Sleep(100 * time.Millisecond)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			srv.Shutdown(ctx)
		}()
	})
	mux.HandleFunc("/mcp", func(w http.ResponseWriter, r *http.Request) {
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

	// On SIGINT/SIGTERM, run Finish() to delete all containers, then shut down.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("orchestrator: caught signal, finalizing...")
		mgr.Finish()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	}()

	log.Printf("orchestrator listening on :%d", port)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
