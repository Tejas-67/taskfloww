package config

import (
	"fmt"
	"strings"
)

// ValidationError aggregates all configuration problems so the user sees every
// issue at once instead of fixing them one restart at a time.
type ValidationError struct {
	Problems []string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("invalid configuration (%d problem(s)):\n  - %s",
		len(e.Problems), strings.Join(e.Problems, "\n  - "))
}

var (
	validLogLevels   = map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	validLogFormats  = map[string]bool{"json": true, "text": true}
	validBackoffKind = map[string]bool{"exponential": true, "fixed": true}
)

// Validate checks required fields, value ranges, and cross-field invariants.
// It returns a *ValidationError listing every problem, or nil if the config is
// usable by BOTH the orchestrator and workers.
func (c *Config) Validate() error {
	var p []string
	add := func(format string, args ...interface{}) { p = append(p, fmt.Sprintf(format, args...)) }

	// logging
	if !validLogLevels[c.Logging.Level] {
		add("logging.level %q must be one of debug|info|warn|error", c.Logging.Level)
	}
	if !validLogFormats[c.Logging.Format] {
		add("logging.format %q must be one of json|text", c.Logging.Format)
	}

	// server
	if c.Server.HTTPAddr == "" {
		add("server.http_addr is required (e.g. \":8080\")")
	}

	// database (orchestrator source of truth)
	if c.Database.URI == "" {
		add("database.uri is required")
	}
	if c.Database.MaxOpenConns <= 0 {
		add("database.max_open_conns must be > 0 (got %d)", c.Database.MaxOpenConns)
	}
	if c.Database.MaxIdleConns < 0 {
		add("database.max_idle_conns must be >= 0 (got %d)", c.Database.MaxIdleConns)
	}
	if c.Database.MaxIdleConns > c.Database.MaxOpenConns {
		add("database.max_idle_conns (%d) must be <= max_open_conns (%d)",
			c.Database.MaxIdleConns, c.Database.MaxOpenConns)
	}

	// broker
	if c.Broker.URI == "" {
		add("broker.uri is required")
	}
	if c.Broker.Prefetch <= 0 {
		add("broker.prefetch must be > 0 (got %d)", c.Broker.Prefetch)
	}

	// queues
	if c.Queues.DefaultExchange == "" {
		add("queues.default_exchange is required")
	}
	if c.Queues.DeadLetterExchange == "" {
		add("queues.dead_letter_exchange is required")
	}
	if len(c.Queues.Definitions) == 0 {
		add("queues.definitions must contain at least one queue")
	}
	queueNames := map[string]bool{}
	for i, q := range c.Queues.Definitions {
		if q.Name == "" {
			add("queues.definitions[%d].name is required", i)
		}
		if q.RoutingKey == "" {
			add("queues.definitions[%d].routing_key is required", i)
		}
		if q.MaxPriority < 0 || q.MaxPriority > 255 {
			add("queues.definitions[%d].max_priority must be 0..255 (got %d)", i, q.MaxPriority)
		}
		if queueNames[q.Name] {
			add("queues.definitions has duplicate name %q", q.Name)
		}
		queueNames[q.Name] = true
	}
	if c.Queues.DeadLetter.Name == "" {
		add("queues.dead_letter.name is required")
	}

	// retry / backoff
	if c.Retry.MaxRetries < 0 {
		add("retry.max_retries must be >= 0 (got %d)", c.Retry.MaxRetries)
	}
	if !validBackoffKind[c.Retry.Backoff.Strategy] {
		add("retry.backoff.strategy %q must be exponential|fixed", c.Retry.Backoff.Strategy)
	}
	if c.Retry.Backoff.BaseSeconds <= 0 {
		add("retry.backoff.base_seconds must be > 0 (got %g)", c.Retry.Backoff.BaseSeconds)
	}
	if c.Retry.Backoff.Strategy == "exponential" && c.Retry.Backoff.Multiplier < 1 {
		add("retry.backoff.multiplier must be >= 1 for exponential (got %g)", c.Retry.Backoff.Multiplier)
	}
	if c.Retry.Backoff.MaxSeconds < c.Retry.Backoff.BaseSeconds {
		add("retry.backoff.max_seconds (%g) must be >= base_seconds (%g)",
			c.Retry.Backoff.MaxSeconds, c.Retry.Backoff.BaseSeconds)
	}

	// heartbeat invariants (the crux of at-least-once fault tolerance)
	if c.Heartbeat.IntervalSeconds <= 0 {
		add("heartbeat.interval_seconds must be > 0 (got %d)", c.Heartbeat.IntervalSeconds)
	}
	if c.Heartbeat.LeaseTTLSeconds <= c.Heartbeat.IntervalSeconds {
		add("heartbeat.lease_ttl_seconds (%d) must be > interval_seconds (%d) to avoid false re-queues",
			c.Heartbeat.LeaseTTLSeconds, c.Heartbeat.IntervalSeconds)
	}
	if c.Heartbeat.ReaperIntervalSeconds <= 0 {
		add("heartbeat.reaper_interval_seconds must be > 0 (got %d)", c.Heartbeat.ReaperIntervalSeconds)
	}
	if c.Heartbeat.ReaperIntervalSeconds > c.Heartbeat.LeaseTTLSeconds {
		add("heartbeat.reaper_interval_seconds (%d) must be <= lease_ttl_seconds (%d) to detect expiry in time",
			c.Heartbeat.ReaperIntervalSeconds, c.Heartbeat.LeaseTTLSeconds)
	}

	// scheduler
	if c.Scheduler.DispatchBatchSize <= 0 {
		add("scheduler.dispatch_batch_size must be > 0 (got %d)", c.Scheduler.DispatchBatchSize)
	}
	if c.Scheduler.PollIntervalMs <= 0 {
		add("scheduler.poll_interval_ms must be > 0 (got %d)", c.Scheduler.PollIntervalMs)
	}

	// task → function mappings (plug-and-play core)
	taskNames := map[string]bool{}
	for i, t := range c.Tasks {
		label := fmt.Sprintf("tasks[%d]", i)
		if t.Name != "" {
			label = fmt.Sprintf("tasks[%q]", t.Name)
		}
		if t.Name == "" {
			add("%s.name is required", label)
		} else if taskNames[t.Name] {
			add("duplicate task name %q", t.Name)
		}
		taskNames[t.Name] = true

		if err := validateHandler(t.Handler); err != nil {
			add("%s.handler %v", label, err)
		}
		if t.Queue != "" && !queueNames[t.Queue] {
			add("%s.queue %q does not match any queues.definitions[].name", label, t.Queue)
		}
		if t.MaxRetries != nil && *t.MaxRetries < 0 {
			add("%s.max_retries must be >= 0 (got %d)", label, *t.MaxRetries)
		}
	}

	if len(p) > 0 {
		return &ValidationError{Problems: p}
	}
	return nil
}

// validateHandler checks the "module:function" shape of a task handler path.
func validateHandler(h string) error {
	if h == "" {
		return fmt.Errorf("is required (expected \"module:function\")")
	}
	mod, fn, ok := strings.Cut(h, ":")
	if !ok || mod == "" || fn == "" {
		return fmt.Errorf("%q must be \"module:function\"", h)
	}
	return nil
}
