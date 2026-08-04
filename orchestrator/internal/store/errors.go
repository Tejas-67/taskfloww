// Package store provides PostgreSQL persistence for TaskFloww. It is the only
// component that writes task state (ADR-0002 / B1). Business logic depends on
// the small interfaces defined by its consumers (e.g. service.Store); the
// concrete Postgres type here satisfies them.
package store

import "errors"

// Sentinel errors returned by the store and mapped to HTTP status codes by the
// API layer.
var (
	// ErrNotFound is returned when a row does not exist.
	ErrNotFound = errors.New("not found")
	// ErrDuplicate is returned on a unique-constraint violation (e.g. a
	// duplicate idempotency_key or schedule name).
	ErrDuplicate = errors.New("duplicate")
	// ErrConflict is returned when a row exists but is not in a state that
	// permits the requested transition (e.g. cancelling a running task).
	ErrConflict = errors.New("conflict")
)
