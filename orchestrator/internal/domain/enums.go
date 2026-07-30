// Package domain defines TaskFloww's core domain types: the task/worker state
// machines and value objects. These mirror the SQL schema in ../../migrations
// exactly (enum values are identical strings) so the database and the Go code
// share one source of truth. The package is storage-agnostic — it imports only
// the standard library and is safe to use from the API, dispatcher, consumer,
// and reaper.
package domain

// TaskState is the lifecycle state of a task. Mirrors SQL enum `task_state`.
//
// Required states from the spec: Queued, Running, Completed, Failed, Retrying.
// The extras (Dispatching, Dead, Cancelled) are operational transitions the
// engine needs internally.
type TaskState string

const (
	// TaskQueued: ready; dispatched once next_run_at <= now().
	TaskQueued TaskState = "queued"
	// TaskDispatching: claimed by an orchestrator and being published (leased).
	TaskDispatching TaskState = "dispatching"
	// TaskRunning: a worker is executing it (leased, heartbeating).
	TaskRunning TaskState = "running"
	// TaskRetrying: failed; waiting for backoff (next_run_at in the future).
	TaskRetrying TaskState = "retrying"
	// TaskCompleted: succeeded (terminal).
	TaskCompleted TaskState = "completed"
	// TaskFailed: terminal, non-retryable failure (terminal).
	TaskFailed TaskState = "failed"
	// TaskDead: retries exhausted; moved to the DLQ (terminal).
	TaskDead TaskState = "dead"
	// TaskCancelled: cancelled before completion (terminal).
	TaskCancelled TaskState = "cancelled"
)

// Valid reports whether s is a known task state.
func (s TaskState) Valid() bool {
	switch s {
	case TaskQueued, TaskDispatching, TaskRunning, TaskRetrying,
		TaskCompleted, TaskFailed, TaskDead, TaskCancelled:
		return true
	default:
		return false
	}
}

// Terminal reports whether s is a final state (no further transitions).
func (s TaskState) Terminal() bool {
	switch s {
	case TaskCompleted, TaskFailed, TaskDead, TaskCancelled:
		return true
	default:
		return false
	}
}

// Dispatchable reports whether a task in this state is eligible for the
// dispatcher due-scan (matches the idx_tasks_due partial index predicate).
func (s TaskState) Dispatchable() bool {
	return s == TaskQueued || s == TaskRetrying
}

// InFlight reports whether a task in this state holds a lease and is watched by
// the reaper (matches the idx_tasks_lease_expiry partial index predicate).
func (s TaskState) InFlight() bool {
	return s == TaskDispatching || s == TaskRunning
}

// ExecutionType is how a task is scheduled. Mirrors SQL enum `execution_type`.
type ExecutionType string

const (
	// ExecImmediate: run as soon as possible (next_run_at = now()).
	ExecImmediate ExecutionType = "immediate"
	// ExecDelayed: run at a future next_run_at.
	ExecDelayed ExecutionType = "delayed"
	// ExecRecurring: a run materialized from a cron schedule (schedule_id set).
	ExecRecurring ExecutionType = "recurring"
)

// Valid reports whether t is a known execution type.
func (t ExecutionType) Valid() bool {
	switch t {
	case ExecImmediate, ExecDelayed, ExecRecurring:
		return true
	default:
		return false
	}
}

// ExecutionState is the state of a single task_executions row. Mirrors SQL enum
// `execution_state`.
type ExecutionState string

const (
	ExecutionDispatched ExecutionState = "dispatched"
	ExecutionRunning    ExecutionState = "running"
	ExecutionSucceeded  ExecutionState = "succeeded"
	ExecutionFailed     ExecutionState = "failed"
)

// Valid reports whether e is a known execution state.
func (e ExecutionState) Valid() bool {
	switch e {
	case ExecutionDispatched, ExecutionRunning, ExecutionSucceeded, ExecutionFailed:
		return true
	default:
		return false
	}
}

// WorkerStatus is a worker's liveness. Mirrors SQL enum `worker_status`.
type WorkerStatus string

const (
	// WorkerAlive: heartbeating normally.
	WorkerAlive WorkerStatus = "alive"
	// WorkerDraining: finishing in-flight work, accepting no new tasks.
	WorkerDraining WorkerStatus = "draining"
	// WorkerDead: missed its heartbeat timeout; its tasks are re-queued.
	WorkerDead WorkerStatus = "dead"
)

// Valid reports whether w is a known worker status.
func (w WorkerStatus) Valid() bool {
	switch w {
	case WorkerAlive, WorkerDraining, WorkerDead:
		return true
	default:
		return false
	}
}
