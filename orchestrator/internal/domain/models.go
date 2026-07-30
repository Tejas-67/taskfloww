package domain

import (
	"encoding/json"
	"time"
)

// These structs mirror the rows defined in ../../migrations. Nullable columns
// use pointer / slice types; JSONB columns use json.RawMessage. They are plain
// data holders — persistence mapping (pgx/sqlc) is wired in a later phase.

// Task is a single runnable unit and the central state-machine row (tasks).
type Task struct {
	ID             string          `json:"id"`
	IdempotencyKey string          `json:"idempotency_key"` // client-supplied "unique id"
	TaskName       string          `json:"task_name"`       // maps to a worker function
	Payload        json.RawMessage `json:"payload"`
	State          TaskState       `json:"state"`
	ExecutionType  ExecutionType   `json:"execution_type"`
	Priority       int16           `json:"priority"`
	MaxRetries     int             `json:"max_retries"`
	Attempt        int             `json:"attempt"`

	NextRunAt time.Time `json:"next_run_at"`
	Queue     *string   `json:"queue,omitempty"`

	// Lease / execution tracking (fault tolerance).
	LockedBy        *string    `json:"locked_by,omitempty"`
	LeaseExpiresAt  *time.Time `json:"lease_expires_at,omitempty"`
	LastHeartbeatAt *time.Time `json:"last_heartbeat_at,omitempty"`
	DispatchedAt    *time.Time `json:"dispatched_at,omitempty"`
	StartedAt       *time.Time `json:"started_at,omitempty"`
	CompletedAt     *time.Time `json:"completed_at,omitempty"`
	LastError       *string    `json:"last_error,omitempty"`

	ScheduleID *string `json:"schedule_id,omitempty"` // set for materialized recurring runs

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// RetriesRemaining reports how many attempts are left before the task is dead.
func (t *Task) RetriesRemaining() int {
	r := t.MaxRetries - t.Attempt
	if r < 0 {
		return 0
	}
	return r
}

// TaskExecution is one attempt to run a task (task_executions). Its ID is the
// execution_id carried in the RabbitMQ message and used for idempotent results.
type TaskExecution struct {
	ID            string          `json:"id"`
	TaskID        string          `json:"task_id"`
	AttemptNumber int             `json:"attempt_number"`
	WorkerID      *string         `json:"worker_id,omitempty"`
	State         ExecutionState  `json:"state"`
	DispatchedAt  time.Time       `json:"dispatched_at"`
	StartedAt     *time.Time      `json:"started_at,omitempty"`
	FinishedAt    *time.Time      `json:"finished_at,omitempty"`
	Error         *string         `json:"error,omitempty"`
	Result        json.RawMessage `json:"result,omitempty"`
}

// Worker is a registered worker node (workers). The orchestrator upserts this
// from heartbeat messages; workers never write it directly (ADR-0002/B1).
type Worker struct {
	ID              string          `json:"id"`
	Hostname        *string         `json:"hostname,omitempty"`
	PID             *int            `json:"pid,omitempty"`
	Queues          []string        `json:"queues"`
	Status          WorkerStatus    `json:"status"`
	Metadata        json.RawMessage `json:"metadata"`
	RegisteredAt    time.Time       `json:"registered_at"`
	LastHeartbeatAt time.Time       `json:"last_heartbeat_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
}

// Schedule is a recurring (cron) definition (schedules). The scheduler
// materializes a Task run per firing.
type Schedule struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	TaskName    string          `json:"task_name"`
	Payload     json.RawMessage `json:"payload"`
	CronExpr    string          `json:"cron_expr"`
	Timezone    string          `json:"timezone"`
	Priority    int16           `json:"priority"`
	MaxRetries  int             `json:"max_retries"`
	Queue       *string         `json:"queue,omitempty"`
	Enabled     bool            `json:"enabled"`
	NextFireAt  time.Time       `json:"next_fire_at"`
	LastFiredAt *time.Time      `json:"last_fired_at,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

// OutboxMessage is a pending broker publish written atomically with a task
// state transition (outbox). The relay publishes it and stamps PublishedAt.
type OutboxMessage struct {
	ID              int64           `json:"id"`
	TaskID          *string         `json:"task_id,omitempty"`
	Exchange        string          `json:"exchange"`
	RoutingKey      string          `json:"routing_key"`
	Payload         json.RawMessage `json:"payload"`
	Headers         json.RawMessage `json:"headers"`
	Priority        int16           `json:"priority"`
	CreatedAt       time.Time       `json:"created_at"`
	PublishedAt     *time.Time      `json:"published_at,omitempty"`
	PublishAttempts int             `json:"publish_attempts"`
}

// Published reports whether the outbox row has been sent to the broker.
func (o *OutboxMessage) Published() bool { return o.PublishedAt != nil }

// DeadLetter is a task that exhausted its retries (dead_letters), retained for
// introspection and replay.
type DeadLetter struct {
	ID                  string          `json:"id"`
	TaskID              *string         `json:"task_id,omitempty"`
	TaskName            string          `json:"task_name"`
	Payload             json.RawMessage `json:"payload"`
	Attempts            int             `json:"attempts"`
	LastError           *string         `json:"last_error,omitempty"`
	OriginalExecutionID *string         `json:"original_execution_id,omitempty"`
	DiedAt              time.Time       `json:"died_at"`
	ReplayedAt          *time.Time      `json:"replayed_at,omitempty"`
}
