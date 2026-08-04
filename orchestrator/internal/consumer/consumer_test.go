package consumer

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/Tejas-67/taskfloww/orchestrator/internal/backoff"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/config"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/domain"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/message"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/store"
)

type applyCall struct {
	executionID, workerID string
	success               bool
	errMsg                string
}

type fakeStore struct {
	applyCalls []applyCall
	applied    bool
	state      domain.TaskState

	upserts  []store.WorkerInput
	renewIDs []string
}

func (f *fakeStore) ApplyResult(_ context.Context, execID, workerID string, success bool, _ json.RawMessage, errMsg string, _ func(int) time.Duration) (bool, domain.TaskState, error) {
	f.applyCalls = append(f.applyCalls, applyCall{execID, workerID, success, errMsg})
	return f.applied, f.state, nil
}

func (f *fakeStore) RenewLeases(_ context.Context, ids []string, _ time.Duration) (int64, error) {
	f.renewIDs = ids
	return int64(len(ids)), nil
}

func (f *fakeStore) UpsertWorker(_ context.Context, w store.WorkerInput) error {
	f.upserts = append(f.upserts, w)
	return nil
}

func newConsumer(fs *fakeStore) *Consumer {
	bo := backoff.New(config.Backoff{Strategy: "exponential", BaseSeconds: 2, Multiplier: 2, MaxSeconds: 300})
	return New(fs, bo, 60*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func mustJSON(t *testing.T, v interface{}) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestHandleResultSuccess(t *testing.T) {
	fs := &fakeStore{applied: true, state: domain.TaskCompleted}
	c := newConsumer(fs)
	body := mustJSON(t, message.Result{
		Type: message.TypeResult, ExecutionID: "exec-1", TaskID: "task-1",
		WorkerID: "w1", Status: message.StatusSucceeded,
	})
	if err := c.handleBody(context.Background(), body); err != nil {
		t.Fatal(err)
	}
	if len(fs.applyCalls) != 1 || !fs.applyCalls[0].success || fs.applyCalls[0].executionID != "exec-1" {
		t.Errorf("unexpected apply calls: %+v", fs.applyCalls)
	}
}

func TestHandleResultFailure(t *testing.T) {
	fs := &fakeStore{applied: true, state: domain.TaskRetrying}
	c := newConsumer(fs)
	body := mustJSON(t, message.Result{
		Type: message.TypeResult, ExecutionID: "exec-2", Status: message.StatusFailed, Error: "boom",
	})
	if err := c.handleBody(context.Background(), body); err != nil {
		t.Fatal(err)
	}
	if fs.applyCalls[0].success || fs.applyCalls[0].errMsg != "boom" {
		t.Errorf("failure not passed through: %+v", fs.applyCalls[0])
	}
}

func TestHandleResultDuplicateIsNoError(t *testing.T) {
	fs := &fakeStore{applied: false} // dedupe
	c := newConsumer(fs)
	body := mustJSON(t, message.Result{Type: message.TypeResult, ExecutionID: "exec-3", Status: message.StatusSucceeded})
	if err := c.handleBody(context.Background(), body); err != nil {
		t.Fatalf("duplicate result should not error: %v", err)
	}
}

func TestHandleHeartbeat(t *testing.T) {
	fs := &fakeStore{}
	c := newConsumer(fs)
	body := mustJSON(t, message.Heartbeat{
		Type: message.TypeHeartbeat, WorkerID: "w1", Hostname: "h", PID: 42, Queues: []string{"tasks.default"},
		InFlight: []message.InFlight{{ExecutionID: "e1", TaskID: "t1"}, {ExecutionID: "e2", TaskID: "t2"}},
	})
	if err := c.handleBody(context.Background(), body); err != nil {
		t.Fatal(err)
	}
	if len(fs.upserts) != 1 || fs.upserts[0].ID != "w1" || fs.upserts[0].PID != 42 {
		t.Errorf("worker upsert wrong: %+v", fs.upserts)
	}
	if len(fs.renewIDs) != 2 || fs.renewIDs[0] != "t1" || fs.renewIDs[1] != "t2" {
		t.Errorf("lease renew ids wrong: %+v", fs.renewIDs)
	}
}

func TestHandleUnknownTypeIgnored(t *testing.T) {
	fs := &fakeStore{}
	c := newConsumer(fs)
	if err := c.handleBody(context.Background(), []byte(`{"type":"nope"}`)); err != nil {
		t.Fatalf("unknown type should be ignored, got %v", err)
	}
	if len(fs.applyCalls) != 0 || len(fs.upserts) != 0 {
		t.Error("no store calls expected for unknown type")
	}
}

func TestHandleMalformedJSON(t *testing.T) {
	c := newConsumer(&fakeStore{})
	if err := c.handleBody(context.Background(), []byte(`{bad`)); err == nil {
		t.Fatal("expected error on malformed JSON")
	}
}
