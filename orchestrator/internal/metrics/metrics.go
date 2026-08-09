// Package metrics defines TaskFloww's Prometheus metrics for the orchestrator.
// Metrics are registered on the default registry via promauto, so any component
// can increment them by referencing the package-level vars; main exposes them
// at /metrics.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	namespace = "taskfloww"
	subsystem = "orchestrator"
)

var (
	// TasksSubmitted counts accepted submissions, by task name and execution type.
	TasksSubmitted = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: subsystem, Name: "tasks_submitted_total",
		Help: "Tasks accepted by the submission API.",
	}, []string{"task_name", "execution_type"})

	// TasksDispatched counts tasks claimed and enqueued by the dispatcher.
	TasksDispatched = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: subsystem, Name: "tasks_dispatched_total",
		Help: "Tasks claimed from Postgres and written to the outbox.",
	})

	// TaskResults counts results applied, by resulting task state
	// (completed | retrying | dead).
	TaskResults = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: subsystem, Name: "task_results_total",
		Help: "Task results applied by the consumer, by resulting state.",
	}, []string{"state"})

	// LeasesReaped counts tasks re-queued or killed by the reaper's lease scan.
	LeasesReaped = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: subsystem, Name: "leases_reaped_total",
		Help: "Expired-lease tasks handled by the reaper, by outcome (requeued|dead).",
	}, []string{"outcome"})

	// SchedulesFired counts recurring schedules materialized into task runs.
	SchedulesFired = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: subsystem, Name: "schedules_fired_total",
		Help: "Recurring schedules fired into task runs by the reaper.",
	})

	// WorkersMarkedDead counts workers flagged dead by the stale-worker scan.
	WorkersMarkedDead = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: subsystem, Name: "workers_marked_dead_total",
		Help: "Workers marked dead due to a lapsed heartbeat.",
	})

	// HeartbeatsReceived counts worker heartbeat messages consumed.
	HeartbeatsReceived = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: subsystem, Name: "heartbeats_received_total",
		Help: "Worker heartbeat messages processed by the consumer.",
	})

	// OutboxPublished counts messages relayed to the broker.
	OutboxPublished = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: subsystem, Name: "outbox_published_total",
		Help: "Outbox rows successfully published to RabbitMQ.",
	})

	// OutboxPublishFailures counts failed publish attempts.
	OutboxPublishFailures = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace, Subsystem: subsystem, Name: "outbox_publish_failures_total",
		Help: "Outbox publish attempts that failed (left for retry).",
	})

	// HTTPRequestDuration is the API request latency histogram.
	HTTPRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Subsystem: subsystem, Name: "http_request_duration_seconds",
		Help:    "HTTP request duration.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "route", "status"})

	// --- Gauges sampled from Postgres by the collector ---

	// TasksActive is the number of non-terminal tasks, by state (queue depth = queued).
	TasksActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace, Subsystem: subsystem, Name: "tasks_active",
		Help: "Non-terminal tasks by state.",
	}, []string{"state"})

	// WorkersAlive is the number of workers currently alive.
	WorkersAlive = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace, Subsystem: subsystem, Name: "workers_alive",
		Help: "Workers currently reporting alive.",
	})

	// OutboxPending is the number of unpublished outbox rows (relay backlog).
	OutboxPending = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace, Subsystem: subsystem, Name: "outbox_pending",
		Help: "Unpublished outbox rows awaiting the relay.",
	})
)

// Handler serves the metrics registry.
func Handler() http.Handler { return promhttp.Handler() }
