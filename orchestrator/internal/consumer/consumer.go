// Package consumer is the orchestrator-side reader of worker control messages
// (results and heartbeats) from the control queue. It is the single writer of
// terminal task state (ADR-0002/B1): it applies results idempotently, renews
// task leases from heartbeats, and refreshes worker liveness.
package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Tejas-67/taskfloww/orchestrator/internal/backoff"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/domain"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/message"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/metrics"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/store"
)

// Store is the persistence surface the consumer needs.
type Store interface {
	ApplyResult(ctx context.Context, executionID, workerID string, success bool, result json.RawMessage, errMsg string, backoffFor func(attempt int) time.Duration) (bool, domain.TaskState, error)
	RenewLeases(ctx context.Context, taskIDs []string, lease time.Duration) (int64, error)
	UpsertWorker(ctx context.Context, w store.WorkerInput) error
}

// Consumer processes control-queue deliveries.
type Consumer struct {
	store   Store
	backoff *backoff.Policy
	lease   time.Duration
	logger  *slog.Logger
}

// New builds a Consumer.
func New(st Store, bo *backoff.Policy, lease time.Duration, logger *slog.Logger) *Consumer {
	return &Consumer{store: st, backoff: bo, lease: lease, logger: logger}
}

// Run consumes deliveries until ctx is cancelled, acking each (poison messages
// are acked and logged so they don't loop forever).
func (c *Consumer) Run(ctx context.Context, deliveries <-chan amqp.Delivery) {
	c.logger.Info("result/heartbeat consumer started")
	for {
		select {
		case <-ctx.Done():
			c.logger.Info("consumer stopped")
			return
		case d, ok := <-deliveries:
			if !ok {
				c.logger.Warn("delivery channel closed")
				return
			}
			if err := c.handleBody(ctx, d.Body); err != nil {
				c.logger.Error("failed to handle control message", "error", err)
			}
			_ = d.Ack(false)
		}
	}
}

// handleBody parses and applies one control message. Separated from AMQP so it
// is unit-testable without a broker.
func (c *Consumer) handleBody(ctx context.Context, body []byte) error {
	var env message.Envelope
	if err := json.Unmarshal(body, &env); err != nil {
		return fmt.Errorf("unmarshaling envelope: %w", err)
	}
	switch env.Type {
	case message.TypeResult:
		return c.handleResult(ctx, body)
	case message.TypeHeartbeat:
		return c.handleHeartbeat(ctx, body)
	default:
		c.logger.Warn("unknown control message type; dropping", "type", env.Type)
		return nil
	}
}

func (c *Consumer) handleResult(ctx context.Context, body []byte) error {
	var r message.Result
	if err := json.Unmarshal(body, &r); err != nil {
		return fmt.Errorf("unmarshaling result: %w", err)
	}
	if r.ExecutionID == "" {
		return fmt.Errorf("result missing execution_id")
	}
	applied, state, err := c.store.ApplyResult(ctx, r.ExecutionID, r.WorkerID,
		r.Status == message.StatusSucceeded, r.Result, r.Error, c.backoff.Next)
	if err != nil {
		return fmt.Errorf("applying result for execution %s: %w", r.ExecutionID, err)
	}
	if !applied {
		c.logger.Debug("duplicate/unknown result ignored", "execution_id", r.ExecutionID)
		return nil
	}
	metrics.TaskResults.WithLabelValues(string(state)).Inc()
	c.logger.Info("applied result",
		"execution_id", r.ExecutionID, "task_id", r.TaskID,
		"status", r.Status, "task_state", state)
	return nil
}

func (c *Consumer) handleHeartbeat(ctx context.Context, body []byte) error {
	var h message.Heartbeat
	if err := json.Unmarshal(body, &h); err != nil {
		return fmt.Errorf("unmarshaling heartbeat: %w", err)
	}
	if h.WorkerID == "" {
		return fmt.Errorf("heartbeat missing worker_id")
	}
	metrics.HeartbeatsReceived.Inc()
	if err := c.store.UpsertWorker(ctx, store.WorkerInput{
		ID: h.WorkerID, Hostname: h.Hostname, PID: h.PID, Queues: h.Queues, Status: domain.WorkerAlive,
	}); err != nil {
		return fmt.Errorf("upserting worker %s: %w", h.WorkerID, err)
	}
	taskIDs := make([]string, 0, len(h.InFlight))
	for _, f := range h.InFlight {
		if f.TaskID != "" {
			taskIDs = append(taskIDs, f.TaskID)
		}
	}
	renewed, err := c.store.RenewLeases(ctx, taskIDs, c.lease)
	if err != nil {
		return fmt.Errorf("renewing leases for worker %s: %w", h.WorkerID, err)
	}
	c.logger.Debug("heartbeat processed", "worker_id", h.WorkerID, "in_flight", len(taskIDs), "renewed", renewed)
	return nil
}
