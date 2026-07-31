// Command orchestrator is the entrypoint for the TaskFloww orchestrator.
//
// Phase 2: boots from the plug-and-play config file (path via -config or
// TASKFLOWW_CONFIG), configures structured logging from it, and serves a
// liveness endpoint with graceful shutdown. Invalid config fails fast. The
// scheduling engine — submission API, SKIP LOCKED dispatcher, result/heartbeat
// consumer, reaper, and outbox relay — is added in later phases (docs/ROADMAP.md).
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Tejas-67/taskfloww/orchestrator/internal/config"
)

// version is overridden at build time via -ldflags "-X main.version=...".
var version = "0.0.0-phase2"

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", getenv("TASKFLOWW_CONFIG", "config/config.example.yaml"),
		"path to the TaskFloww config file (or set TASKFLOWW_CONFIG)")
	flag.Parse()

	// A bootstrap logger for errors that happen before config is applied.
	boot := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	cfg, err := config.Load(configPath)
	if err != nil {
		// Fail fast with the full list of problems.
		boot.Error("failed to load configuration", "path", configPath, "error", err.Error())
		os.Exit(1)
	}

	logger := newLogger(cfg.Logging)
	slog.SetDefault(logger)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	// NOTE: GET /metrics (Prometheus) is wired in Phase 5 (observability).

	srv := &http.Server{
		Addr:              cfg.Server.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Cancel the root context on shutdown signals so we can drain gracefully —
	// a prerequisite for at-least-once delivery (no work lost on redeploys).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("orchestrator starting",
			"version", version,
			"environment", cfg.App.Environment,
			"addr", cfg.Server.HTTPAddr,
			"config_path", configPath,
			// credentials are redacted before logging
			"database", config.RedactURI(cfg.Database.URI),
			"broker", config.RedactURI(cfg.Broker.URI),
			"tasks_configured", len(cfg.Tasks),
			"lease_ttl", cfg.Heartbeat.LeaseTTL().String(),
			"heartbeat_interval", cfg.Heartbeat.Interval().String(),
		)
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

// newLogger builds a slog.Logger from the logging config (level + json/text).
func newLogger(lc config.Logging) *slog.Logger {
	level := slog.LevelInfo
	switch strings.ToLower(lc.Level) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	opts := &slog.HandlerOptions{Level: level}
	if strings.ToLower(lc.Format) == "text" {
		return slog.New(slog.NewTextHandler(os.Stdout, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, opts))
}

// getenv returns the value of the environment variable named by key, or
// fallback when it is unset or empty.
func getenv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}
