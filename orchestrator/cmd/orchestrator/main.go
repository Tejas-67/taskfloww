// Command orchestrator is the entrypoint for the TaskFloww orchestrator.
//
// Phase 3a: loads config, connects to PostgreSQL (source of truth), and serves
// the REST submission API (chi) behind the SchedulerService interface, with
// config-driven structured logging and graceful shutdown. The SKIP LOCKED
// dispatcher, result/heartbeat consumer, reaper, and outbox relay are added in
// later phases (docs/ROADMAP.md).
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
	"sync"
	"syscall"
	"time"

	"github.com/Tejas-67/taskfloww/orchestrator/internal/api"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/backoff"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/broker"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/config"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/consumer"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/dispatcher"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/relay"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/service"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/store"
)

// version is overridden at build time via -ldflags "-X main.version=...".
var version = "0.0.0-phase3c"

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

	// Cancel the root context on shutdown signals so we can drain gracefully —
	// a prerequisite for at-least-once delivery (no work lost on redeploys).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Connect to PostgreSQL (the source of truth). Fail fast if unavailable.
	dbCtx, cancelDB := context.WithTimeout(ctx, 10*time.Second)
	st, err := store.Connect(dbCtx, cfg.Database.URI,
		cfg.Database.MaxOpenConns, cfg.Database.MaxIdleConns, cfg.Database.ConnMaxIdleSeconds)
	cancelDB()
	if err != nil {
		logger.Error("failed to connect to database", "error", err.Error())
		os.Exit(1)
	}
	defer st.Close()

	// Connect to RabbitMQ (transport) and declare the queue/DLX topology.
	brk, err := broker.Connect(cfg.Broker.URI, cfg.Broker.ConnectionName)
	if err != nil {
		logger.Error("failed to connect to broker", "error", err.Error())
		os.Exit(1)
	}
	defer brk.Close()
	if err := brk.DeclareTopology(cfg.Queues, cfg.Control); err != nil {
		logger.Error("failed to declare broker topology", "error", err.Error())
		os.Exit(1)
	}

	// Control-queue consumer stream (worker results + heartbeats).
	ctrlDeliveries, ctrlCh, err := brk.Consume(cfg.Control.Queue, "orchestrator", cfg.Broker.Prefetch)
	if err != nil {
		logger.Error("failed to consume control queue", "error", err.Error())
		os.Exit(1)
	}
	defer ctrlCh.Close()

	svc := service.New(st, cfg)
	router := api.NewRouter(svc, logger)

	srv := &http.Server{
		Addr:              cfg.Server.HTTPAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Background engine loops: dispatcher (DB → outbox), relay (outbox → broker),
	// and the result/heartbeat consumer (broker → DB).
	disp := dispatcher.New(st, cfg, logger)
	rel := relay.New(st, brk, logger, cfg.Scheduler.DispatchBatchSize, cfg.Scheduler.PollInterval())
	cons := consumer.New(st, backoff.New(cfg.Retry.Backoff), cfg.Heartbeat.LeaseTTL(), logger)
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); disp.Run(ctx) }()
	go func() { defer wg.Done(); rel.Run(ctx) }()
	go func() { defer wg.Done(); cons.Run(ctx, ctrlDeliveries) }()

	go func() {
		logger.Info("orchestrator starting",
			"version", version,
			"environment", cfg.App.Environment,
			"addr", cfg.Server.HTTPAddr,
			"config_path", configPath,
			// credentials are redacted before logging
			"database", config.RedactURI(cfg.Database.URI),
			"broker", config.RedactURI(cfg.Broker.URI),
			"instance", disp.InstanceID(),
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
	}
	wg.Wait() // let dispatcher/relay finish their current tick
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
