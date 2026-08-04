//go:build integration

// Integration tests for the Postgres store. Run against a live database:
//
//	TASKFLOWW_TEST_DB_URI="postgres://...sslmode=disable" \
//	  go test -tags=integration ./internal/store/
//
// Skipped automatically when TASKFLOWW_TEST_DB_URI is unset.
package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Tejas-67/taskfloww/orchestrator/internal/domain"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/store"
)

func dsn(t *testing.T) string {
	v := os.Getenv("TASKFLOWW_TEST_DB_URI")
	if v == "" {
		t.Skip("TASKFLOWW_TEST_DB_URI not set; skipping integration test")
	}
	return v
}

func TestPostgresLifecycle(t *testing.T) {
	ctx := context.Background()
	p, err := store.Connect(ctx, dsn(t), 5, 1, 300)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer p.Close()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())

	// insert immediate task — exercises jsonb/enum/uuid casts
	task := &domain.Task{
		IdempotencyKey: "it-" + suffix,
		TaskName:       "send_email",
		Payload:        json.RawMessage(`{"to":"a@b.com"}`),
		State:          domain.TaskQueued,
		ExecutionType:  domain.ExecImmediate,
		Priority:       5,
		MaxRetries:     3,
		NextRunAt:      time.Now().UTC(),
	}
	if err := p.InsertTask(ctx, task); err != nil {
		t.Fatalf("InsertTask: %v", err)
	}
	if task.ID == "" || task.State != domain.TaskQueued {
		t.Fatalf("generated fields not populated: %+v", task)
	}

	// read back
	got, err := p.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if got.TaskName != "send_email" || got.Priority != 5 || string(got.Payload) != `{"to": "a@b.com"}` {
		t.Errorf("round-trip mismatch: %+v payload=%s", got, got.Payload)
	}

	// duplicate idempotency key -> ErrDuplicate
	dup := *task
	dup.ID = ""
	if err := p.InsertTask(ctx, &dup); !errors.Is(err, store.ErrDuplicate) {
		t.Errorf("expected ErrDuplicate, got %v", err)
	}

	// lookup by idempotency key
	byKey, err := p.GetTaskByIdempotencyKey(ctx, task.IdempotencyKey)
	if err != nil || byKey.ID != task.ID {
		t.Errorf("GetTaskByIdempotencyKey: %v id=%v", err, byKey)
	}

	// cancel (queued -> cancelled)
	cancelled, err := p.CancelTask(ctx, task.ID)
	if err != nil || cancelled.State != domain.TaskCancelled {
		t.Errorf("CancelTask: %v state=%v", err, cancelled.State)
	}
	// cancel again -> conflict (already terminal)
	if _, err := p.CancelTask(ctx, task.ID); !errors.Is(err, store.ErrConflict) {
		t.Errorf("expected ErrConflict on second cancel, got %v", err)
	}

	// missing id -> not found
	if _, err := p.GetTask(ctx, uuid.NewString()); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}

	// schedule insert
	sched := &domain.Schedule{
		Name:       "sched-" + suffix,
		TaskName:   "send_email",
		Payload:    json.RawMessage(`{}`),
		CronExpr:   "*/5 * * * *",
		Timezone:   "UTC",
		Priority:   0,
		MaxRetries: 5,
		Enabled:    true,
		NextFireAt: time.Now().Add(5 * time.Minute).UTC(),
	}
	if err := p.InsertSchedule(ctx, sched); err != nil || sched.ID == "" {
		t.Errorf("InsertSchedule: %v id=%q", err, sched.ID)
	}
}
