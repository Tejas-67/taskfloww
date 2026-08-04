package message

import (
	"encoding/json"
	"time"
)

// Control message types published by workers to the control exchange and
// consumed by the orchestrator. Every control message carries a Type so a
// single queue can multiplex them.
const (
	TypeResult    = "result"
	TypeHeartbeat = "heartbeat"
)

// ResultStatus is the outcome a worker reports for an execution.
type ResultStatus string

const (
	StatusSucceeded ResultStatus = "succeeded"
	StatusFailed    ResultStatus = "failed"
)

// Result is published when a worker finishes (or fails) an execution. It is
// applied idempotently by the orchestrator using ExecutionID.
type Result struct {
	Type        string          `json:"type"` // = TypeResult
	ExecutionID string          `json:"execution_id"`
	TaskID      string          `json:"task_id"`
	TaskName    string          `json:"task_name"`
	WorkerID    string          `json:"worker_id"`
	Attempt     int             `json:"attempt"`
	Status      ResultStatus    `json:"status"`
	Result      json.RawMessage `json:"result,omitempty"`
	Error       string          `json:"error,omitempty"`
	FinishedAt  time.Time       `json:"finished_at"`
}

// InFlight identifies one execution a worker is currently running.
type InFlight struct {
	ExecutionID string `json:"execution_id"`
	TaskID      string `json:"task_id"`
}

// Heartbeat is published periodically by a worker. It renews the leases of the
// listed in-flight tasks and refreshes the worker's liveness row.
type Heartbeat struct {
	Type      string     `json:"type"` // = TypeHeartbeat
	WorkerID  string     `json:"worker_id"`
	Hostname  string     `json:"hostname"`
	PID       int        `json:"pid"`
	Queues    []string   `json:"queues"`
	InFlight  []InFlight `json:"in_flight"`
	Timestamp time.Time  `json:"timestamp"`
}

// Envelope peeks at the Type of an incoming control message.
type Envelope struct {
	Type string `json:"type"`
}
