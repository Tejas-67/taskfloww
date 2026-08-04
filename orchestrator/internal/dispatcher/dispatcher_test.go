package dispatcher

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/Tejas-67/taskfloww/orchestrator/internal/config"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/domain"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/message"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/store"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func testCfg() *config.Config {
	return &config.Config{
		Queues: config.Queues{
			DefaultExchange: "tf.direct",
			Definitions: []config.QueueDef{
				{Name: "tasks.high", RoutingKey: "rk.high", MaxPriority: 10},
				{Name: "tasks.default", RoutingKey: "rk.default", MaxPriority: 10},
			},
		},
	}
}

const taskUUID = "11111111-1111-1111-1111-111111111111"

func TestBuildOutboxResolvesRoutingAndClampsPriority(t *testing.T) {
	d := New(nil, testCfg(), testLogger())
	q := "tasks.high"
	task := &domain.Task{
		ID: taskUUID, TaskName: "send_email", Payload: json.RawMessage(`{"x":1}`),
		Priority: 20 /* over max */, Attempt: 1, MaxRetries: 3, Queue: &q,
	}
	msg, err := d.buildOutbox(task, "exec-1")
	if err != nil {
		t.Fatal(err)
	}
	if msg.Exchange != "tf.direct" || msg.RoutingKey != "rk.high" {
		t.Errorf("routing wrong: exchange=%s rk=%s", msg.Exchange, msg.RoutingKey)
	}
	if msg.Priority != 10 {
		t.Errorf("priority = %d, want clamped to 10", msg.Priority)
	}
	if msg.TaskID == nil || *msg.TaskID != taskUUID {
		t.Errorf("task id not set: %+v", msg.TaskID)
	}
	var m message.Task
	if err := json.Unmarshal(msg.Payload, &m); err != nil {
		t.Fatal(err)
	}
	if m.ExecutionID != "exec-1" || m.TaskID != taskUUID || m.TaskName != "send_email" || m.Priority != 10 {
		t.Errorf("message body wrong: %+v", m)
	}
}

func TestBuildOutboxFallsBackToDefaultQueue(t *testing.T) {
	d := New(nil, testCfg(), testLogger())
	task := &domain.Task{ID: taskUUID, TaskName: "x", Queue: nil} // no queue → first definition
	msg, err := d.buildOutbox(task, "e")
	if err != nil {
		t.Fatal(err)
	}
	if msg.RoutingKey != "rk.high" {
		t.Errorf("expected default (first) queue routing rk.high, got %s", msg.RoutingKey)
	}
}

func TestClampPriority(t *testing.T) {
	cases := []struct{ p, max, want int16 }{
		{5, 10, 5}, {20, 10, 10}, {-3, 10, 0}, {7, 0, 7},
	}
	for _, c := range cases {
		if got := clampPriority(c.p, c.max); got != c.want {
			t.Errorf("clampPriority(%d,%d)=%d want %d", c.p, c.max, got, c.want)
		}
	}
}

// fakeStore invokes the build callback (as the real store does inside its tx)
// so the dispatcher's routing/serialization is exercised end-to-end.
type fakeStore struct {
	built []domain.OutboxMessage
}

func (f *fakeStore) ClaimDueTasks(_ context.Context, _ int, _ time.Duration, lockedBy string, build store.BuildOutboxFunc) ([]store.Claimed, error) {
	task := &domain.Task{ID: taskUUID, TaskName: "send_email", Attempt: 1}
	msg, err := build(task, "exec-9")
	if err != nil {
		return nil, err
	}
	f.built = append(f.built, msg)
	return []store.Claimed{{Task: task, ExecutionID: "exec-9"}}, nil
}

func TestTickInvokesClaimAndBuild(t *testing.T) {
	fs := &fakeStore{}
	d := New(fs, testCfg(), testLogger())
	if err := d.tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(fs.built) != 1 {
		t.Fatalf("expected 1 built outbox message, got %d", len(fs.built))
	}
	if fs.built[0].RoutingKey != "rk.high" {
		t.Errorf("unexpected routing: %s", fs.built[0].RoutingKey)
	}
}
