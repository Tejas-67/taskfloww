//go:build integration

package store_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Tejas-67/taskfloww/orchestrator/internal/domain"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/store"
)

func TestDeadLetterListAndReplay(t *testing.T) {
	ctx := context.Background()
	p, err := store.Connect(ctx, dsn(t), 5, 1, 300)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// Make a task dead: claim (max_retries=0) then report a failure → dead + dead_letters.
	key := fmt.Sprintf("dlq-%d", time.Now().UnixNano())
	taskID, execID := claimOne(t, ctx, p, key, 0)
	applied, state, err := p.ApplyResult(ctx, execID, "w1", false, nil, "boom", func(int) time.Duration { return time.Second })
	if err != nil || !applied || state != domain.TaskDead {
		t.Fatalf("expected task dead: state=%v err=%v", state, err)
	}

	// It appears in the dead-letter list; find ours by task_id.
	list, err := p.ListDeadLetters(ctx, 200, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	var dlID string
	for _, d := range list {
		if d.TaskID != nil && *d.TaskID == taskID {
			dlID = d.ID
		}
	}
	if dlID == "" {
		t.Fatalf("dead letter for task %s not found in list", taskID)
	}

	// GetDeadLetter returns it.
	dl, err := p.GetDeadLetter(ctx, dlID)
	if err != nil || dl.TaskName != "send_email" || dl.LastError == nil || *dl.LastError != "boom" {
		t.Fatalf("GetDeadLetter wrong: %+v err=%v", dl, err)
	}

	// Replay → task back to queued with a fresh attempt budget.
	task, err := p.ReplayDeadLetter(ctx, dlID)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if task.State != domain.TaskQueued || task.Attempt != 0 {
		t.Errorf("replayed task not reset: state=%v attempt=%d", task.State, task.Attempt)
	}
	got, _ := p.GetTask(ctx, taskID)
	if got.State != domain.TaskQueued || got.CompletedAt != nil {
		t.Errorf("task not re-queued: %+v", got)
	}

	// The dead letter is marked replayed and excluded from the default list.
	dl2, _ := p.GetDeadLetter(ctx, dlID)
	if dl2.ReplayedAt == nil {
		t.Error("dead letter not marked replayed")
	}
	// A second replay conflicts.
	if _, err := p.ReplayDeadLetter(ctx, dlID); !errors.Is(err, store.ErrConflict) {
		t.Errorf("expected ErrConflict on second replay, got %v", err)
	}
	// Missing id → not found.
	if _, err := p.GetDeadLetter(ctx, "00000000-0000-0000-0000-000000000000"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}
