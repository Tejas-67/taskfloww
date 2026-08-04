//go:build integration

package store_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Tejas-67/taskfloww/orchestrator/internal/domain"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/store"
)

func contains(claimed []store.Claimed, id string) bool {
	for _, c := range claimed {
		if c.Task.ID == id {
			return true
		}
	}
	return false
}

func TestClaimDueTasksAndPublishOutbox(t *testing.T) {
	ctx := context.Background()
	d := dsn(t)
	p, err := store.Connect(ctx, d, 5, 1, 300)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer p.Close()
	raw, err := pgxpool.New(ctx, d)
	if err != nil {
		t.Fatalf("raw pool: %v", err)
	}
	defer raw.Close()

	sfx := time.Now().UnixNano()
	mkTask := func(key string, runAt time.Time) *domain.Task {
		return &domain.Task{
			IdempotencyKey: key, TaskName: "send_email", Payload: json.RawMessage(`{}`),
			State: domain.TaskQueued, ExecutionType: domain.ExecImmediate,
			Priority: 5, MaxRetries: 3, NextRunAt: runAt,
		}
	}
	due := mkTask(fmt.Sprintf("due-%d", sfx), time.Now().Add(-time.Minute).UTC())
	future := mkTask(fmt.Sprintf("fut-%d", sfx), time.Now().Add(time.Hour).UTC())
	if err := p.InsertTask(ctx, due); err != nil {
		t.Fatal(err)
	}
	if err := p.InsertTask(ctx, future); err != nil {
		t.Fatal(err)
	}

	build := func(tk *domain.Task, execID string) (domain.OutboxMessage, error) {
		id := tk.ID
		return domain.OutboxMessage{
			TaskID: &id, Exchange: "tf.direct", RoutingKey: "rk",
			Payload: []byte(fmt.Sprintf(`{"execution_id":%q}`, execID)), Priority: tk.Priority,
		}, nil
	}

	claimed, err := p.ClaimDueTasks(ctx, 100, time.Minute, "test-inst", build)
	if err != nil {
		t.Fatalf("ClaimDueTasks: %v", err)
	}
	if !contains(claimed, due.ID) {
		t.Errorf("due task was not claimed")
	}
	if contains(claimed, future.ID) {
		t.Errorf("future (not-due) task must NOT be claimed")
	}

	// due task transitioned + leased + attempt incremented
	got, err := p.GetTask(ctx, due.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != domain.TaskDispatching || got.Attempt != 1 || got.LeaseExpiresAt == nil ||
		got.LockedBy == nil || *got.LockedBy != "test-inst" {
		t.Errorf("claim did not update task correctly: %+v", got)
	}

	// execution ledger + outbox rows created in the same tx
	var n int
	raw.QueryRow(ctx, `SELECT count(*) FROM task_executions WHERE task_id=$1::uuid AND attempt_number=1`, due.ID).Scan(&n)
	if n != 1 {
		t.Errorf("expected 1 execution row, got %d", n)
	}
	raw.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE task_id=$1::uuid AND published_at IS NULL`, due.ID).Scan(&n)
	if n != 1 {
		t.Errorf("expected 1 unpublished outbox row, got %d", n)
	}

	// a task in 'dispatching' is not re-claimed (SKIP LOCKED + state predicate)
	claimed2, err := p.ClaimDueTasks(ctx, 100, time.Minute, "test-inst", build)
	if err != nil {
		t.Fatal(err)
	}
	if contains(claimed2, due.ID) {
		t.Errorf("already-dispatching task must not be re-claimed")
	}

	// relay: publish drains the outbox and stamps published_at
	var publishedForDue int
	pub := func(m domain.OutboxMessage) error {
		if m.TaskID != nil && *m.TaskID == due.ID {
			publishedForDue++
		}
		return nil
	}
	if _, err := p.PublishOutbox(ctx, 1000, pub); err != nil {
		t.Fatalf("PublishOutbox: %v", err)
	}
	if publishedForDue != 1 {
		t.Errorf("expected due task's outbox published once, got %d", publishedForDue)
	}
	raw.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE task_id=$1::uuid AND published_at IS NOT NULL`, due.ID).Scan(&n)
	if n != 1 {
		t.Errorf("expected due task's outbox row marked published, got %d", n)
	}
}
