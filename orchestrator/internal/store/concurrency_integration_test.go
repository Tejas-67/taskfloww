//go:build integration

// Multi-instance concurrency tests. These prove the core distributed-systems
// guarantee behind a stateless, horizontally-scaled orchestrator: several
// instances hammering the SAME rows simultaneously never double-process a task.
//
// Each test fires K goroutines (simulating K orchestrator/relay/reaper
// instances) through a start barrier so they contend at the same instant, then
// asserts per-task ledger integrity (keyed on this test's own task ids, so the
// assertions are independent of any other rows already in the shared DB).
package store_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Tejas-67/taskfloww/orchestrator/internal/domain"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/store"
)

// ccBuild is the dispatcher-supplied outbox builder used by the claim tests.
func ccBuild(tk *domain.Task, execID string) (domain.OutboxMessage, error) {
	id := tk.ID
	return domain.OutboxMessage{
		TaskID: &id, Exchange: "tf.direct", RoutingKey: "rk",
		Payload: []byte(fmt.Sprintf(`{"execution_id":%q}`, execID)), Priority: tk.Priority,
	}, nil
}

// insertDueTasks inserts n due queued tasks and returns the set of their ids.
func insertDueTasks(t *testing.T, ctx context.Context, p *store.Postgres, prefix string, n, maxRetries int) map[string]bool {
	t.Helper()
	sfx := time.Now().UnixNano()
	ids := make(map[string]bool, n)
	for i := 0; i < n; i++ {
		task := &domain.Task{
			IdempotencyKey: fmt.Sprintf("%s-%d-%d", prefix, sfx, i),
			TaskName:       "send_email", Payload: json.RawMessage(`{}`),
			State: domain.TaskQueued, ExecutionType: domain.ExecImmediate,
			Priority: 5, MaxRetries: maxRetries, NextRunAt: time.Now().Add(-time.Minute).UTC(),
		}
		if err := p.InsertTask(ctx, task); err != nil {
			t.Fatalf("InsertTask %d: %v", i, err)
		}
		ids[task.ID] = true
	}
	return ids
}

// TestConcurrentClaimNoDoubleDispatch proves that N due tasks claimed by K
// contending orchestrator instances are each claimed exactly once — no task is
// dispatched twice, and each ends with exactly one execution + one outbox row.
func TestConcurrentClaimNoDoubleDispatch(t *testing.T) {
	ctx := context.Background()
	d := dsn(t)
	p, err := store.Connect(ctx, d, 16, 2, 300)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer p.Close()
	raw, err := pgxpool.New(ctx, d)
	if err != nil {
		t.Fatalf("raw pool: %v", err)
	}
	defer raw.Close()

	const (
		nTasks     = 40
		nInstances = 8
		claimBatch = 2 // small batches force instances to interleave and contend
	)
	mine := insertDueTasks(t, ctx, p, "cc-claim", nTasks, 3)

	var (
		mu         sync.Mutex
		claimCount = make(map[string]int, nTasks) // my task id -> times claimed across all instances
	)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < nInstances; w++ {
		wg.Add(1)
		go func(inst int) {
			defer wg.Done()
			<-start
			lockedBy := fmt.Sprintf("inst-%d", inst)
			for {
				claimed, err := p.ClaimDueTasks(ctx, claimBatch, time.Minute, lockedBy, ccBuild)
				if err != nil {
					t.Errorf("inst %d ClaimDueTasks: %v", inst, err)
					return
				}
				if len(claimed) == 0 {
					return // drained
				}
				mu.Lock()
				for _, c := range claimed {
					if mine[c.Task.ID] {
						claimCount[c.Task.ID]++
					}
				}
				mu.Unlock()
			}
		}(w)
	}
	close(start) // fire all instances simultaneously
	wg.Wait()

	// Every one of my tasks was claimed exactly once — the no-double-dispatch guarantee.
	if len(claimCount) != nTasks {
		t.Fatalf("expected %d distinct tasks claimed, got %d", nTasks, len(claimCount))
	}
	for id, c := range claimCount {
		if c != 1 {
			t.Errorf("task %s claimed %d times (want 1) — DOUBLE DISPATCH", id, c)
		}
	}

	// Ledger integrity: each task is dispatching with exactly one execution + one outbox row.
	for id := range mine {
		var execs, outbox int
		raw.QueryRow(ctx, `SELECT count(*) FROM task_executions WHERE task_id=$1::uuid`, id).Scan(&execs)
		raw.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE task_id=$1::uuid`, id).Scan(&outbox)
		if execs != 1 || outbox != 1 {
			t.Errorf("task %s: executions=%d outbox=%d (want 1/1)", id, execs, outbox)
		}
		got, err := p.GetTask(ctx, id)
		if err != nil {
			t.Fatalf("GetTask %s: %v", id, err)
		}
		if got.State != domain.TaskDispatching || got.Attempt != 1 {
			t.Errorf("task %s: state=%v attempt=%d (want dispatching/1)", id, got.State, got.Attempt)
		}
	}
}

// TestConcurrentPublishOutboxNoDoubleSend proves that M pending outbox rows
// drained by K contending relay instances are each published exactly once.
func TestConcurrentPublishOutboxNoDoubleSend(t *testing.T) {
	ctx := context.Background()
	d := dsn(t)
	p, err := store.Connect(ctx, d, 16, 2, 300)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer p.Close()
	raw, err := pgxpool.New(ctx, d)
	if err != nil {
		t.Fatalf("raw pool: %v", err)
	}
	defer raw.Close()

	const (
		nTasks     = 30
		nInstances = 8
		pubBatch   = 3
	)
	// Seed outbox rows by claiming due tasks (one outbox row per task).
	mine := insertDueTasks(t, ctx, p, "cc-pub", nTasks, 3)
	claimed, err := p.ClaimDueTasks(ctx, 1000, time.Minute, "seeder", ccBuild)
	if err != nil {
		t.Fatalf("seed claim: %v", err)
	}
	seeded := 0
	for _, c := range claimed {
		if mine[c.Task.ID] {
			seeded++
		}
	}
	if seeded != nTasks {
		t.Fatalf("seeding: expected %d of my tasks claimed, got %d", nTasks, seeded)
	}

	var (
		mu       sync.Mutex
		sendsFor = make(map[string]int, nTasks) // my task id -> times its outbox published
	)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < nInstances; w++ {
		wg.Add(1)
		go func(inst int) {
			defer wg.Done()
			<-start
			publish := func(m domain.OutboxMessage) error {
				if m.TaskID != nil {
					mu.Lock()
					if mine[*m.TaskID] {
						sendsFor[*m.TaskID]++
					}
					mu.Unlock()
				}
				return nil
			}
			for {
				n, err := p.PublishOutbox(ctx, pubBatch, publish)
				if err != nil {
					t.Errorf("inst %d PublishOutbox: %v", inst, err)
					return
				}
				if n == 0 {
					return // drained
				}
			}
		}(w)
	}
	close(start)
	wg.Wait()

	// Each of my outbox rows was published exactly once — no duplicate broker sends.
	if len(sendsFor) != nTasks {
		t.Fatalf("expected %d distinct outbox rows published, got %d", nTasks, len(sendsFor))
	}
	for id, c := range sendsFor {
		if c != 1 {
			t.Errorf("task %s outbox published %d times (want 1) — DOUBLE PUBLISH", id, c)
		}
	}
	// And every one is stamped published in the DB.
	for id := range mine {
		var pending int
		raw.QueryRow(ctx,
			`SELECT count(*) FROM outbox WHERE task_id=$1::uuid AND published_at IS NULL`, id).Scan(&pending)
		if pending != 0 {
			t.Errorf("task %s has %d unpublished outbox rows (want 0)", id, pending)
		}
	}
}

// TestConcurrentReapNoDoubleRequeue proves that when N leases expire at once
// (workers presumed crashed) and K reaper instances contend, each task is
// re-queued exactly once with a single failed execution — no corruption.
func TestConcurrentReapNoDoubleRequeue(t *testing.T) {
	ctx := context.Background()
	d := dsn(t)
	p, err := store.Connect(ctx, d, 16, 2, 300)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer p.Close()
	raw, err := pgxpool.New(ctx, d)
	if err != nil {
		t.Fatalf("raw pool: %v", err)
	}
	defer raw.Close()

	const (
		nTasks     = 30
		nInstances = 8
		reapBatch  = 2
	)
	// Claim my tasks (dispatching + lease), then force every lease into the past
	// so all N look like crashed workers simultaneously.
	mine := insertDueTasks(t, ctx, p, "cc-reap", nTasks, 5) // ample retries -> retrying, never dead
	if _, err := p.ClaimDueTasks(ctx, 1000, time.Minute, "seeder", ccBuild); err != nil {
		t.Fatalf("seed claim: %v", err)
	}
	ids := make([]string, 0, nTasks)
	for id := range mine {
		ids = append(ids, id)
	}
	if _, err := raw.Exec(ctx,
		`UPDATE tasks SET lease_expires_at = now() - interval '1 minute' WHERE id = ANY($1::uuid[])`, ids,
	); err != nil {
		t.Fatalf("expire leases: %v", err)
	}

	backoff := func(int) time.Duration { return 30 * time.Second } // retrying, next_run in the future

	start := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < nInstances; w++ {
		wg.Add(1)
		go func(inst int) {
			defer wg.Done()
			<-start
			for {
				rq, dead, err := p.ReapExpiredLeases(ctx, reapBatch, backoff)
				if err != nil {
					t.Errorf("inst %d ReapExpiredLeases: %v", inst, err)
					return
				}
				if rq == 0 && dead == 0 {
					return // drained
				}
			}
		}(w)
	}
	close(start)
	wg.Wait()

	// Every one of my tasks re-queued exactly once: retrying, lease cleared, and
	// exactly one failed execution row for the crashed attempt.
	for id := range mine {
		got, err := p.GetTask(ctx, id)
		if err != nil {
			t.Fatalf("GetTask %s: %v", id, err)
		}
		if got.State != domain.TaskRetrying || got.LeaseExpiresAt != nil {
			t.Errorf("task %s: state=%v lease=%v (want retrying/nil)", id, got.State, got.LeaseExpiresAt)
		}
		var failed int
		raw.QueryRow(ctx,
			`SELECT count(*) FROM task_executions WHERE task_id=$1::uuid AND state='failed'`, id).Scan(&failed)
		if failed != 1 {
			t.Errorf("task %s: %d failed executions (want exactly 1) — DOUBLE REAP", id, failed)
		}
	}
}
