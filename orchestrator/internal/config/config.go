// Package config defines TaskFloww's plug-and-play configuration: one YAML file
// drives both the Go orchestrator and the Python workers. Values may be
// overridden by environment variables (prefix TASKFLOWW_, nested keys joined by
// "__", e.g. TASKFLOWW_DATABASE__URI) and YAML values support ${VAR:-default}
// interpolation. Invalid configuration fails fast with a combined error listing
// every problem (see Validate).
package config

import "time"

// Config is the full configuration document. Sections used only by workers
// (broker, queues, retry, heartbeat, tasks, logging, metrics) and only by the
// orchestrator (database, server, scheduler) coexist in one file; each side
// reads what it needs.
type Config struct {
	App       App           `koanf:"app"`
	Logging   Logging       `koanf:"logging"`
	Server    Server        `koanf:"server"`
	Metrics   Metrics       `koanf:"metrics"`
	Database  Database      `koanf:"database"`
	Broker    Broker        `koanf:"broker"`
	Queues    Queues        `koanf:"queues"`
	Control   Control       `koanf:"control"`
	Retry     Retry         `koanf:"retry"`
	Heartbeat Heartbeat     `koanf:"heartbeat"`
	Scheduler Scheduler     `koanf:"scheduler"`
	Tasks     []TaskMapping `koanf:"tasks"`
}

// App holds identity/environment metadata.
type App struct {
	Name        string `koanf:"name"`
	Environment string `koanf:"environment"` // development | staging | production
}

// Logging controls structured logging.
type Logging struct {
	Level  string `koanf:"level"`  // debug | info | warn | error
	Format string `koanf:"format"` // json | text
}

// Server is the orchestrator HTTP server (health, metrics, future API).
type Server struct {
	HTTPAddr string `koanf:"http_addr"`
}

// Metrics configures Prometheus exposition.
type Metrics struct {
	Enabled    bool   `koanf:"enabled"`
	Path       string `koanf:"path"`
	WorkerPort int    `koanf:"worker_port"` // port each worker exposes /metrics on
}

// Database is the PostgreSQL source of truth (orchestrator only).
type Database struct {
	URI                string `koanf:"uri"`
	MaxOpenConns       int    `koanf:"max_open_conns"`
	MaxIdleConns       int    `koanf:"max_idle_conns"`
	ConnMaxIdleSeconds int    `koanf:"conn_max_idle_seconds"`
}

// Broker is the RabbitMQ connection.
type Broker struct {
	URI            string `koanf:"uri"`
	Prefetch       int    `koanf:"prefetch"` // per-consumer in-flight limit (backpressure)
	ConnectionName string `koanf:"connection_name"`
}

// QueueDef is one work queue and its routing key.
type QueueDef struct {
	Name        string `koanf:"name"`
	RoutingKey  string `koanf:"routing_key"`
	MaxPriority int    `koanf:"max_priority"`
}

// DeadLetter is the DLQ queue bound to the dead-letter exchange.
type DeadLetter struct {
	Name       string `koanf:"name"`
	RoutingKey string `koanf:"routing_key"`
}

// Queues is the broker topology.
type Queues struct {
	DefaultExchange    string     `koanf:"default_exchange"`
	DeadLetterExchange string     `koanf:"dead_letter_exchange"`
	Definitions        []QueueDef `koanf:"definitions"`
	DeadLetter         DeadLetter `koanf:"dead_letter"`
}

// Control is the exchange/queue for worker→orchestrator messages (results and
// heartbeats). Both message types share one queue, multiplexed by routing key.
type Control struct {
	Exchange            string `koanf:"exchange"`
	Queue               string `koanf:"queue"`
	ResultRoutingKey    string `koanf:"result_routing_key"`
	HeartbeatRoutingKey string `koanf:"heartbeat_routing_key"`
}

// Backoff is the retry backoff policy.
type Backoff struct {
	Strategy    string  `koanf:"strategy"` // exponential | fixed
	BaseSeconds float64 `koanf:"base_seconds"`
	Multiplier  float64 `koanf:"multiplier"`
	MaxSeconds  float64 `koanf:"max_seconds"`
	Jitter      bool    `koanf:"jitter"`
}

// Retry is the default retry policy (per-task overridable).
type Retry struct {
	MaxRetries int     `koanf:"max_retries"`
	Backoff    Backoff `koanf:"backoff"`
}

// Heartbeat holds fault-tolerance timings. Invariant: LeaseTTL > Interval and
// ReaperInterval <= LeaseTTL (enforced in Validate).
type Heartbeat struct {
	IntervalSeconds       int `koanf:"interval_seconds"`
	LeaseTTLSeconds       int `koanf:"lease_ttl_seconds"`
	ReaperIntervalSeconds int `koanf:"reaper_interval_seconds"`
}

// Scheduler tunes the dispatcher poll loop (orchestrator only).
type Scheduler struct {
	DispatchBatchSize int `koanf:"dispatch_batch_size"`
	PollIntervalMs    int `koanf:"poll_interval_ms"`
}

// TaskMapping maps a task name to a worker function (the plug-and-play core).
// Handler is an importable "module:function" path in the worker environment.
type TaskMapping struct {
	Name       string `koanf:"name"`
	Handler    string `koanf:"handler"`
	Queue      string `koanf:"queue"`
	MaxRetries *int   `koanf:"max_retries"` // nil → inherit Retry.MaxRetries
}

// --- convenience accessors -------------------------------------------------

// Interval returns the heartbeat cadence.
func (h Heartbeat) Interval() time.Duration {
	return time.Duration(h.IntervalSeconds) * time.Second
}

// LeaseTTL returns the task/worker lease duration (visibility timeout).
func (h Heartbeat) LeaseTTL() time.Duration {
	return time.Duration(h.LeaseTTLSeconds) * time.Second
}

// ReaperInterval returns how often the reaper scans for expired leases.
func (h Heartbeat) ReaperInterval() time.Duration {
	return time.Duration(h.ReaperIntervalSeconds) * time.Second
}

// PollInterval returns the dispatcher poll cadence.
func (s Scheduler) PollInterval() time.Duration {
	return time.Duration(s.PollIntervalMs) * time.Millisecond
}

// EffectiveMaxRetries returns the task's max retries, falling back to def when
// the task does not override it.
func (t TaskMapping) EffectiveMaxRetries(def int) int {
	if t.MaxRetries == nil {
		return def
	}
	return *t.MaxRetries
}

// TaskByName returns the mapping for a task name.
func (c *Config) TaskByName(name string) (TaskMapping, bool) {
	for _, t := range c.Tasks {
		if t.Name == name {
			return t, true
		}
	}
	return TaskMapping{}, false
}

// HandlerMap returns task-name → handler-path for building a worker registry.
func (c *Config) HandlerMap() map[string]string {
	m := make(map[string]string, len(c.Tasks))
	for _, t := range c.Tasks {
		m[t.Name] = t.Handler
	}
	return m
}

// defaults returns a Config pre-populated with sensible defaults. Required
// fields with no safe default (database.uri, broker.uri, queue definitions) are
// left zero and caught by Validate.
func defaults() Config {
	return Config{
		App:     App{Name: "taskfloww", Environment: "development"},
		Logging: Logging{Level: "info", Format: "json"},
		Server:  Server{HTTPAddr: ":8080"},
		Metrics: Metrics{Enabled: true, Path: "/metrics", WorkerPort: 9100},
		Database: Database{
			MaxOpenConns: 20, MaxIdleConns: 10, ConnMaxIdleSeconds: 300,
		},
		Broker: Broker{Prefetch: 32, ConnectionName: "taskfloww"},
		Queues: Queues{DefaultExchange: "taskfloww.direct", DeadLetterExchange: "taskfloww.dlx"},
		Control: Control{
			Exchange: "taskfloww.control", Queue: "taskfloww.control",
			ResultRoutingKey: "result", HeartbeatRoutingKey: "heartbeat",
		},
		Retry: Retry{MaxRetries: 5, Backoff: Backoff{
			Strategy: "exponential", BaseSeconds: 2, Multiplier: 2, MaxSeconds: 300, Jitter: true,
		}},
		Heartbeat: Heartbeat{IntervalSeconds: 10, LeaseTTLSeconds: 60, ReaperIntervalSeconds: 15},
		Scheduler: Scheduler{DispatchBatchSize: 100, PollIntervalMs: 500},
	}
}
