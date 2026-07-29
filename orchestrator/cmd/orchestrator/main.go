// Command orchestrator is the entrypoint for the TaskFloww orchestrator.
//
// Phase 0 scaffold: a minimal, dependency-free skeleton that boots a structured
// (JSON) logger and an HTTP server exposing a liveness endpoint, with graceful
// shutdown on SIGINT/SIGTERM. The scheduling engine — submission API,
// SKIP LOCKED dispatcher, result/heartbeat consumer, reaper, and outbox relay —
// is added in later phases (see docs/ROADMAP.md).
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// version is overridden at build time via -ldflags "-X main.version=...".
var version = "0.0.0-phase0"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	addr := getenv("TASKFLOWW_HTTP_ADDR", ":8080")

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	// NOTE: GET /metrics (Prometheus) is wired in Phase 5 (observability).

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Cancel the root context on shutdown signals so we can drain gracefully —
	// a prerequisite for at-least-once delivery (no work lost on redeploys).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("orchestrator starting", "version", version, "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server failed", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received, draining connections")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
		os.Exit(1)
	}
	logger.Info("orchestrator stopped cleanly")
}

// getenv returns the value of the environment variable named by key, or
// fallback when it is unset or empty.
func getenv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}
