// Package relay drains the transactional outbox to RabbitMQ. It is the only
// component that turns committed outbox rows into broker messages, giving
// exactly-one-publish-per-committed-transition semantics (ADR-0001). Multiple
// instances are safe via FOR UPDATE SKIP LOCKED in the store.
package relay

import (
	"context"
	"log/slog"
	"time"

	"github.com/Tejas-67/taskfloww/orchestrator/internal/domain"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/metrics"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/store"
)

// Store is the persistence surface the relay needs.
type Store interface {
	PublishOutbox(ctx context.Context, batch int, publish store.PublishFunc) (int, error)
}

// Publisher sends a message to the broker (broker.RabbitMQ satisfies this).
type Publisher interface {
	Publish(ctx context.Context, exchange, routingKey string, priority uint8, body []byte) error
}

// Relay periodically publishes unpublished outbox rows.
type Relay struct {
	store  Store
	pub    Publisher
	logger *slog.Logger
	batch  int
	poll   time.Duration
}

// New builds a Relay. batch is reused from the dispatcher batch size; poll is
// the outbox drain cadence.
func New(st Store, pub Publisher, logger *slog.Logger, batch int, poll time.Duration) *Relay {
	return &Relay{store: st, pub: pub, logger: logger, batch: batch, poll: poll}
}

// Run drains the outbox until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) {
	r.logger.Info("outbox relay started", "batch", r.batch, "poll", r.poll.String())
	ticker := time.NewTicker(r.poll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			r.logger.Info("outbox relay stopped")
			return
		case <-ticker.C:
			if err := r.tick(ctx); err != nil && ctx.Err() == nil {
				r.logger.Error("relay tick failed", "error", err)
			}
		}
	}
}

func (r *Relay) tick(ctx context.Context) error {
	n, err := r.store.PublishOutbox(ctx, r.batch, func(m domain.OutboxMessage) error {
		perr := r.pub.Publish(ctx, m.Exchange, m.RoutingKey, toAMQPPriority(m.Priority), m.Payload)
		if perr != nil {
			metrics.OutboxPublishFailures.Inc()
			r.logger.Warn("outbox publish failed; will retry",
				"outbox_id", m.ID, "exchange", m.Exchange, "routing_key", m.RoutingKey, "error", perr)
		}
		return perr
	})
	if err != nil {
		return err
	}
	if n > 0 {
		metrics.OutboxPublished.Add(float64(n))
		r.logger.Info("relayed outbox messages", "published", n)
	}
	return nil
}

// toAMQPPriority clamps an int16 priority into the AMQP 0..255 range.
func toAMQPPriority(p int16) uint8 {
	switch {
	case p < 0:
		return 0
	case p > 255:
		return 255
	default:
		return uint8(p)
	}
}
