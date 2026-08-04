// Package dispatcher claims due tasks from PostgreSQL and enqueues them into the
// transactional outbox. It performs no broker I/O — the relay drains the outbox
// (ADR-0001). Multiple instances run safely: claims use FOR UPDATE SKIP LOCKED.
package dispatcher

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"

	"github.com/Tejas-67/taskfloww/orchestrator/internal/config"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/domain"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/message"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/store"
)

// Store is the persistence surface the dispatcher needs.
type Store interface {
	ClaimDueTasks(ctx context.Context, batch int, lease time.Duration, lockedBy string, build store.BuildOutboxFunc) ([]store.Claimed, error)
}

// Dispatcher polls for due tasks and enqueues them to the outbox.
type Dispatcher struct {
	store      Store
	cfg        *config.Config
	logger     *slog.Logger
	instanceID string
	batch      int
	poll       time.Duration
	lease      time.Duration
	now        func() time.Time
}

// New builds a Dispatcher with a unique instance id (used as the task lease
// owner) and timings from config.
func New(st Store, cfg *config.Config, logger *slog.Logger) *Dispatcher {
	host, _ := os.Hostname()
	return &Dispatcher{
		store:      st,
		cfg:        cfg,
		logger:     logger,
		instanceID: fmt.Sprintf("orch-%s-%d-%s", host, os.Getpid(), uuid.NewString()[:8]),
		batch:      cfg.Scheduler.DispatchBatchSize,
		poll:       cfg.Scheduler.PollInterval(),
		lease:      cfg.Heartbeat.LeaseTTL(),
		now:        func() time.Time { return time.Now().UTC() },
	}
}

// InstanceID is this dispatcher's lease owner id.
func (d *Dispatcher) InstanceID() string { return d.instanceID }

// Run polls until ctx is cancelled.
func (d *Dispatcher) Run(ctx context.Context) {
	d.logger.Info("dispatcher started", "instance", d.instanceID, "batch", d.batch, "poll", d.poll.String())
	ticker := time.NewTicker(d.poll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			d.logger.Info("dispatcher stopped")
			return
		case <-ticker.C:
			if err := d.tick(ctx); err != nil && ctx.Err() == nil {
				d.logger.Error("dispatch tick failed", "error", err)
			}
		}
	}
}

func (d *Dispatcher) tick(ctx context.Context) error {
	claimed, err := d.store.ClaimDueTasks(ctx, d.batch, d.lease, d.instanceID, d.buildOutbox)
	if err != nil {
		return err
	}
	if len(claimed) > 0 {
		d.logger.Info("dispatched tasks", "count", len(claimed))
	}
	return nil
}

// buildOutbox resolves a claimed task's routing from config and serializes the
// worker message. Runs inside the claim transaction.
func (d *Dispatcher) buildOutbox(t *domain.Task, executionID string) (domain.OutboxMessage, error) {
	queueName := ""
	if t.Queue != nil {
		queueName = *t.Queue
	}
	qdef, ok := d.cfg.ResolveQueue(queueName)
	if !ok {
		return domain.OutboxMessage{}, fmt.Errorf("no queue configured for task %q", t.TaskName)
	}
	priority := clampPriority(t.Priority, int16(qdef.MaxPriority))

	body, err := json.Marshal(message.Task{
		TaskID:      t.ID,
		ExecutionID: executionID,
		TaskName:    t.TaskName,
		Payload:     t.Payload,
		Attempt:     t.Attempt,
		MaxRetries:  t.MaxRetries,
		Priority:    priority,
		EnqueuedAt:  d.now(),
	})
	if err != nil {
		return domain.OutboxMessage{}, fmt.Errorf("marshaling message: %w", err)
	}
	taskID := t.ID
	return domain.OutboxMessage{
		TaskID:     &taskID,
		Exchange:   d.cfg.Queues.DefaultExchange,
		RoutingKey: qdef.RoutingKey,
		Payload:    body,
		Priority:   priority,
	}, nil
}

// clampPriority bounds a task priority to [0, max].
func clampPriority(p, max int16) int16 {
	if p < 0 {
		return 0
	}
	if max > 0 && p > max {
		return max
	}
	return p
}
