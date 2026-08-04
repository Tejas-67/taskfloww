// Package message defines the wire contract for task messages published to
// RabbitMQ. The Go dispatcher marshals a Task into the outbox payload; the
// Python worker (Phase 4) parses the same shape. Keep this in sync with the
// worker SDK.
package message

import (
	"encoding/json"
	"time"
)

// Task is the JSON body delivered to a worker for one execution attempt.
type Task struct {
	TaskID      string          `json:"task_id"`
	ExecutionID string          `json:"execution_id"` // task_executions.id — used for idempotent results
	TaskName    string          `json:"task_name"`    // maps to a worker function
	Payload     json.RawMessage `json:"payload"`
	Attempt     int             `json:"attempt"` // 1-based attempt number
	MaxRetries  int             `json:"max_retries"`
	Priority    int16           `json:"priority"`
	EnqueuedAt  time.Time       `json:"enqueued_at"`
}
