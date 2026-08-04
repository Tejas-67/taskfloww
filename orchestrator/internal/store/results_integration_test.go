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

// claimOne inserts a due task and claims it, returning the task id + execution id.
func claimOne(t *testing.T, ctx context.Context, p *store.Postgres, key string, maxRetries int) (string, string) {
	t.Helper()
	task := &domain.Task{
		IdempotencyKey: key, TaskName: "send_email", Payload: json.RawMessage(`{}`),
		State: domain.TaskQueued, ExecutionType: domain.ExecImmediate,
		Priority: 0, MaxRetries: maxRetries, NextRunAt: time.Now().Add(-time.Minute).UTC(),
	}
	if err := p.InsertTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	build := func(tk *domain.Task, execID string) (domain.OutboxMessage, error) {
		id := tk.ID
		return domain.OutboxMessage{TaskID: &id, Exchange: "x", RoutingKey: "r", Payload: []byte(`{}`)}, nil
	}
	claimed, err := p.ClaimDueTasks(ctx, 100, time.Minute, "inst", build)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range claimed {
		if c.Task.IdempotencyKey == key {
			return c.Task.ID, c.ExecutionID
		}
	}
	t.Fatalf("task %s not claimed", key)
	return "", ""
}

func fixedBackoff(time.Duration) func(int) time.Duration {
	return func(int) time.Duration { return 30 * time.Second }
}

func TestApplyResultSuccessAndDedupe(t *testing.T) {
	ctx := context.Background()
	p, err := store.Connect(ctx, dsn(t), 5, 1, 300)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	taskID, execID := claimOne(t, ctx, p, fmt.Sprintf("res-ok-%d", time.Now().UnixNano()), 3)

	applied, state, err := p.ApplyResult(ctx, execID, "w1", true, json.RawMessage(`{"ok":true}`), "", fixedBackoff(0))
	if err != nil || !applied || state != domain.TaskCompleted {
		t.Fatalf("apply success: applied=%v state=%v err=%v", applied, state, err)
	}
	got, _ := p.GetTask(ctx, taskID)
	if got.State != domain.TaskCompleted || got.LeaseExpiresAt != nil || got.LockedBy != nil {
		t.Errorf("completed task not finalized: %+v", got)
	}
	// dedupe: second apply is a no-op
	applied2, _, err := p.ApplyResult(ctx, execID, "w1", true, nil, "", fixedBackoff(0))
	if err != nil || applied2 {
		t.Errorf("duplicate apply should be no-op: applied=%v err=%v", applied2, err)
	}
}

func TestApplyResultFailureRetryThenDead(t *testing.T) {
	ctx := context.Background()
	p, err := store.Connect(ctx, dsn(t), 5, 1, 300)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	raw, _ := pgxpool.New(ctx, dsn(t))
	defer raw.Close()

	// max_retries = 1 → first failure retries, second failure dies.
	key := fmt.Sprintf("res-fail-%d", time.Now().UnixNano())
	taskID, execID := claimOne(t, ctx, p, key, 1)

	applied, state, err := p.ApplyResult(ctx, execID, "w1", false, nil, "boom", fixedBackoff(0))
	if err != nil || !applied || state != domain.TaskRetrying {
		t.Fatalf("first failure should retry: state=%v err=%v", state, err)
	}
	got, _ := p.GetTask(ctx, taskID)
	if got.State != domain.TaskRetrying || got.LeaseExpiresAt != nil || !got.NextRunAt.After(time.Now()) {
		t.Errorf("retry not scheduled in future: %+v", got)
	}

	// re-claim (retrying → dispatching, attempt=2) and fail again → dead
	// bump next_run_at to now so it is due again
	raw.Exec(ctx, `UPDATE tasks SET next_run_at = now() WHERE id=$1::uuid`, taskID)
	build := func(tk *domain.Task, e string) (domain.OutboxMessage, error) {
		id := tk.ID
		return domain.OutboxMessage{TaskID: &id, Exchange: "x", RoutingKey: "r", Payload: []byte(`{}`)}, nil
	}
	claimed, _ := p.ClaimDueTasks(ctx, 100, time.Minute, "inst", build)
	var exec2 string
	for _, c := range claimed {
		if c.Task.ID == taskID {
			exec2 = c.ExecutionID
		}
	}
	if exec2 == "" {
		t.Fatal("retrying task was not re-claimed")
	}

	applied, state, err = p.ApplyResult(ctx, exec2, "w1", false, nil, "boom again", fixedBackoff(0))
	if err != nil || !applied || state != domain.TaskDead {
		t.Fatalf("exhausted failure should be dead: state=%v err=%v", state, err)
	}
	got, _ = p.GetTask(ctx, taskID)
	if got.State != domain.TaskDead {
		t.Errorf("task state = %v, want dead", got.State)
	}
	var deadCount int
	raw.QueryRow(ctx, `SELECT count(*) FROM dead_letters WHERE task_id=$1::uuid`, taskID).Scan(&deadCount)
	if deadCount != 1 {
		t.Errorf("expected 1 dead_letters row, got %d", deadCount)
	}
}

func TestRenewLeasesAndUpsertWorker(t *testing.T) {
	ctx := context.Background()
	p, err := store.Connect(ctx, dsn(t), 5, 1, 300)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	taskID, _ := claimOne(t, ctx, p, fmt.Sprintf("res-hb-%d", time.Now().UnixNano()), 3)

	// worker upsert
	if err := p.UpsertWorker(ctx, store.WorkerInput{
		ID: "worker-1", Hostname: "h", PID: 7, Queues: []string{"tasks.default"}, Status: domain.WorkerAlive,
	}); err != nil {
		t.Fatal(err)
	}
	// idempotent second upsert
	if err := p.UpsertWorker(ctx, store.WorkerInput{ID: "worker-1", Status: domain.WorkerAlive}); err != nil {
		t.Fatal(err)
	}

	// renew lease → dispatching becomes running, lease set
	n, err := p.RenewLeases(ctx, []string{taskID}, 90*time.Second)
	if err != nil || n != 1 {
		t.Fatalf("renew: n=%d err=%v", n, err)
	}
	got, _ := p.GetTask(ctx, taskID)
	if got.State != domain.TaskRunning || got.LeaseExpiresAt == nil {
		t.Errorf("lease not renewed / not running: %+v", got)
	}
}
