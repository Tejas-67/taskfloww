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

func TestReapExpiredLeaseRequeues(t *testing.T) {
	ctx := context.Background()
	p, err := store.Connect(ctx, dsn(t), 5, 1, 300)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	raw, _ := pgxpool.New(ctx, dsn(t))
	defer raw.Close()

	// max_retries=3 → an expired lease should re-queue (retrying).
	taskID, _ := claimOne(t, ctx, p, fmt.Sprintf("reap-rq-%d", time.Now().UnixNano()), 3)
	// simulate the worker having gone silent: force the lease into the past
	if _, err := raw.Exec(ctx, `UPDATE tasks SET lease_expires_at = now() - interval '1 minute' WHERE id=$1::uuid`, taskID); err != nil {
		t.Fatal(err)
	}

	rq, dead, err := p.ReapExpiredLeases(ctx, 100, func(int) time.Duration { return 5 * time.Second })
	if err != nil {
		t.Fatal(err)
	}
	if rq < 1 || dead != 0 {
		t.Errorf("expected >=1 requeued, 0 dead; got rq=%d dead=%d", rq, dead)
	}
	got, _ := p.GetTask(ctx, taskID)
	if got.State != domain.TaskRetrying || got.LeaseExpiresAt != nil || !got.NextRunAt.After(time.Now()) {
		t.Errorf("task not re-queued for retry: %+v", got)
	}
	// the in-flight execution was marked failed
	var execState string
	raw.QueryRow(ctx, `SELECT state FROM task_executions WHERE task_id=$1::uuid AND attempt_number=1`, taskID).Scan(&execState)
	if execState != "failed" {
		t.Errorf("expected stale execution failed, got %q", execState)
	}
}

func TestReapExpiredLeaseDeadWhenExhausted(t *testing.T) {
	ctx := context.Background()
	p, err := store.Connect(ctx, dsn(t), 5, 1, 300)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	raw, _ := pgxpool.New(ctx, dsn(t))
	defer raw.Close()

	// max_retries=0 → attempt 1 with no retries left → dead.
	taskID, _ := claimOne(t, ctx, p, fmt.Sprintf("reap-dead-%d", time.Now().UnixNano()), 0)
	raw.Exec(ctx, `UPDATE tasks SET lease_expires_at = now() - interval '1 minute' WHERE id=$1::uuid`, taskID)

	rq, dead, err := p.ReapExpiredLeases(ctx, 100, func(int) time.Duration { return time.Second })
	if err != nil {
		t.Fatal(err)
	}
	if dead < 1 {
		t.Errorf("expected >=1 dead; got rq=%d dead=%d", rq, dead)
	}
	got, _ := p.GetTask(ctx, taskID)
	if got.State != domain.TaskDead {
		t.Errorf("task state = %v, want dead", got.State)
	}
	var n int
	raw.QueryRow(ctx, `SELECT count(*) FROM dead_letters WHERE task_id=$1::uuid`, taskID).Scan(&n)
	if n != 1 {
		t.Errorf("expected 1 dead_letters row, got %d", n)
	}
}

func TestFireDueSchedules(t *testing.T) {
	ctx := context.Background()
	p, err := store.Connect(ctx, dsn(t), 5, 1, 300)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	raw, _ := pgxpool.New(ctx, dsn(t))
	defer raw.Close()

	name := fmt.Sprintf("sched-fire-%d", time.Now().UnixNano())
	sched := &domain.Schedule{
		Name: name, TaskName: "send_email", Payload: json.RawMessage(`{"k":1}`),
		CronExpr: "*/5 * * * *", Timezone: "UTC", Priority: 3, MaxRetries: 2, Enabled: true,
		NextFireAt: time.Now().Add(-time.Minute).UTC(), // due
	}
	if err := p.InsertSchedule(ctx, sched); err != nil {
		t.Fatal(err)
	}

	// deterministic nextFire: 5 minutes later
	nextFire := func(_, _ string, after time.Time) (time.Time, error) { return after.Add(5 * time.Minute), nil }
	fired, err := p.FireDueSchedules(ctx, 100, nextFire)
	if err != nil {
		t.Fatal(err)
	}
	if fired < 1 {
		t.Fatalf("expected >=1 fired, got %d", fired)
	}

	// a recurring task run was materialized for this schedule
	var (
		state, etype string
		prio         int16
	)
	err = raw.QueryRow(ctx,
		`SELECT state, execution_type, priority FROM tasks WHERE schedule_id=$1::uuid`, sched.ID,
	).Scan(&state, &etype, &prio)
	if err != nil {
		t.Fatalf("materialized task not found: %v", err)
	}
	if state != "queued" || etype != "recurring" || prio != 3 {
		t.Errorf("materialized task wrong: state=%s type=%s prio=%d", state, etype, prio)
	}

	// schedule advanced into the future
	var nextAt time.Time
	var lastFired *time.Time
	raw.QueryRow(ctx, `SELECT next_fire_at, last_fired_at FROM schedules WHERE id=$1::uuid`, sched.ID).Scan(&nextAt, &lastFired)
	if !nextAt.After(time.Now()) || lastFired == nil {
		t.Errorf("schedule not advanced: next=%v lastFired=%v", nextAt, lastFired)
	}
}

func TestMarkStaleWorkers(t *testing.T) {
	ctx := context.Background()
	p, err := store.Connect(ctx, dsn(t), 5, 1, 300)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	raw, _ := pgxpool.New(ctx, dsn(t))
	defer raw.Close()

	wid := fmt.Sprintf("stale-%d", time.Now().UnixNano())
	if err := p.UpsertWorker(ctx, store.WorkerInput{ID: wid, Status: domain.WorkerAlive}); err != nil {
		t.Fatal(err)
	}
	// backdate the heartbeat well beyond the timeout
	raw.Exec(ctx, `UPDATE workers SET last_heartbeat_at = now() - interval '5 minutes' WHERE id=$1`, wid)

	n, err := p.MarkStaleWorkers(ctx, 30*time.Second)
	if err != nil || n < 1 {
		t.Fatalf("MarkStaleWorkers: n=%d err=%v", n, err)
	}
	var status string
	raw.QueryRow(ctx, `SELECT status FROM workers WHERE id=$1`, wid).Scan(&status)
	if status != "dead" {
		t.Errorf("worker status = %q, want dead", status)
	}
}
