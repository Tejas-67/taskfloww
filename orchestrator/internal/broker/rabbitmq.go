// Package broker is the RabbitMQ transport. It declares the exchange/queue
// topology (priority work queues + a dead-letter exchange) and publishes
// messages with publisher confirms so the outbox relay only marks a row
// published once the broker has acknowledged it.
package broker

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Tejas-67/taskfloww/orchestrator/internal/config"
)

// RabbitMQ is a broker connection with a confirming publish channel.
type RabbitMQ struct {
	conn *amqp.Connection
	ch   *amqp.Channel
	mu   sync.Mutex // amqp channels are not safe for concurrent publishers
}

// Connect dials RabbitMQ and opens a channel in confirm mode.
func Connect(uri, connectionName string) (*RabbitMQ, error) {
	conn, err := amqp.DialConfig(uri, amqp.Config{
		Properties: amqp.Table{"connection_name": connectionName},
		Heartbeat:  10 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("dialing broker: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("opening channel: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("enabling publisher confirms: %w", err)
	}
	return &RabbitMQ{conn: conn, ch: ch}, nil
}

// DeclareTopology declares the durable exchanges and queues:
//   - the default direct exchange for work,
//   - the dead-letter exchange (DLX) + DLQ,
//   - each work queue with x-max-priority and dead-lettering to the DLX,
//     bound to the default exchange by its routing key,
//   - the control exchange + queue (results/heartbeats), bound by both routing keys.
func (r *RabbitMQ) DeclareTopology(q config.Queues, ctrl config.Control) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.ch.ExchangeDeclare(q.DefaultExchange, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declaring exchange %q: %w", q.DefaultExchange, err)
	}
	if err := r.ch.ExchangeDeclare(q.DeadLetterExchange, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declaring DLX %q: %w", q.DeadLetterExchange, err)
	}

	// Dead-letter queue.
	if _, err := r.ch.QueueDeclare(q.DeadLetter.Name, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declaring DLQ %q: %w", q.DeadLetter.Name, err)
	}
	if err := r.ch.QueueBind(q.DeadLetter.Name, q.DeadLetter.RoutingKey, q.DeadLetterExchange, false, nil); err != nil {
		return fmt.Errorf("binding DLQ: %w", err)
	}

	// Work queues.
	for _, def := range q.Definitions {
		args := amqp.Table{
			"x-max-priority":            int32(def.MaxPriority),
			"x-dead-letter-exchange":    q.DeadLetterExchange,
			"x-dead-letter-routing-key": q.DeadLetter.RoutingKey,
		}
		if _, err := r.ch.QueueDeclare(def.Name, true, false, false, false, args); err != nil {
			return fmt.Errorf("declaring queue %q: %w", def.Name, err)
		}
		if err := r.ch.QueueBind(def.Name, def.RoutingKey, q.DefaultExchange, false, nil); err != nil {
			return fmt.Errorf("binding queue %q: %w", def.Name, err)
		}
	}

	// Control exchange/queue (worker → orchestrator).
	if err := r.ch.ExchangeDeclare(ctrl.Exchange, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declaring control exchange %q: %w", ctrl.Exchange, err)
	}
	if _, err := r.ch.QueueDeclare(ctrl.Queue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declaring control queue %q: %w", ctrl.Queue, err)
	}
	for _, rk := range []string{ctrl.ResultRoutingKey, ctrl.HeartbeatRoutingKey} {
		if err := r.ch.QueueBind(ctrl.Queue, rk, ctrl.Exchange, false, nil); err != nil {
			return fmt.Errorf("binding control queue (%s): %w", rk, err)
		}
	}
	return nil
}

// Consume opens a dedicated channel (with prefetch) and returns a delivery
// stream for queue. The caller acks/nacks each delivery and closes the returned
// channel on shutdown.
func (r *RabbitMQ) Consume(queue, consumerTag string, prefetch int) (<-chan amqp.Delivery, *amqp.Channel, error) {
	ch, err := r.conn.Channel()
	if err != nil {
		return nil, nil, fmt.Errorf("opening consume channel: %w", err)
	}
	if err := ch.Qos(prefetch, 0, false); err != nil {
		_ = ch.Close()
		return nil, nil, fmt.Errorf("setting qos: %w", err)
	}
	deliveries, err := ch.Consume(queue, consumerTag, false /* manual ack */, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		return nil, nil, fmt.Errorf("consuming %q: %w", queue, err)
	}
	return deliveries, ch, nil
}

// Publish sends a persistent, prioritized message and waits for the broker's
// confirm. An error (or unconfirmed publish) is returned so the caller leaves
// the outbox row unpublished for retry. The publish channel is transparently
// reopened if a prior channel-level error (e.g. a bad exchange) closed it, so a
// single poison message cannot wedge the relay permanently.
func (r *RabbitMQ) Publish(ctx context.Context, exchange, routingKey string, priority uint8, body []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.ch == nil || r.ch.IsClosed() {
		if err := r.reopenLocked(); err != nil {
			return fmt.Errorf("reopening publish channel: %w", err)
		}
	}

	conf, err := r.ch.PublishWithDeferredConfirmWithContext(ctx, exchange, routingKey,
		false, false, amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Priority:     priority,
			Timestamp:    time.Now(),
			Body:         body,
		})
	if err != nil {
		// A channel-level exception (bad exchange, etc.) closes the channel;
		// reopen so the next publish is not affected.
		if r.ch.IsClosed() {
			_ = r.reopenLocked()
		}
		return fmt.Errorf("publishing to %s/%s: %w", exchange, routingKey, err)
	}
	ok, err := conf.WaitContext(ctx)
	if err != nil {
		if r.ch.IsClosed() {
			_ = r.reopenLocked()
		}
		return fmt.Errorf("awaiting publish confirm: %w", err)
	}
	if !ok {
		return fmt.Errorf("publish to %s/%s was nacked by broker", exchange, routingKey)
	}
	return nil
}

// reopenLocked opens a fresh confirming channel. Callers must hold r.mu.
func (r *RabbitMQ) reopenLocked() error {
	ch, err := r.conn.Channel()
	if err != nil {
		return err
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return err
	}
	r.ch = ch
	return nil
}

// Close tears down the channel and connection.
func (r *RabbitMQ) Close() {
	if r.ch != nil {
		_ = r.ch.Close()
	}
	if r.conn != nil {
		_ = r.conn.Close()
	}
}
