package relay

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/Tejas-67/taskfloww/orchestrator/internal/domain"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/store"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// fakePublisher records what it was asked to publish.
type fakePublisher struct {
	calls []published
	err   error
}

type published struct {
	exchange, routingKey string
	priority             uint8
	body                 []byte
}

func (f *fakePublisher) Publish(_ context.Context, exchange, rk string, priority uint8, body []byte) error {
	if f.err != nil {
		return f.err
	}
	f.calls = append(f.calls, published{exchange, rk, priority, body})
	return nil
}

// fakeStore drives the publish callback like the real store would, counting
// successes.
type fakeStore struct {
	msgs []domain.OutboxMessage
}

func (f *fakeStore) PublishOutbox(_ context.Context, _ int, publish store.PublishFunc) (int, error) {
	n := 0
	for _, m := range f.msgs {
		if err := publish(m); err != nil {
			continue
		}
		n++
	}
	return n, nil
}

func TestTickPublishesOutboxMessages(t *testing.T) {
	fs := &fakeStore{msgs: []domain.OutboxMessage{
		{Exchange: "tf.direct", RoutingKey: "rk.high", Priority: 7, Payload: []byte(`{"a":1}`)},
	}}
	fp := &fakePublisher{}
	r := New(fs, fp, testLogger(), 100, 0)

	if err := r.tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(fp.calls) != 1 {
		t.Fatalf("expected 1 publish, got %d", len(fp.calls))
	}
	c := fp.calls[0]
	if c.exchange != "tf.direct" || c.routingKey != "rk.high" || c.priority != 7 {
		t.Errorf("unexpected publish args: %+v", c)
	}
}

func TestTickPropagatesPublishErrorAsNoOpCount(t *testing.T) {
	fs := &fakeStore{msgs: []domain.OutboxMessage{{Exchange: "e", RoutingKey: "r"}}}
	fp := &fakePublisher{err: errors.New("broker down")}
	r := New(fs, fp, testLogger(), 100, 0)
	// store swallows per-message publish errors (leaves rows unpublished); tick succeeds.
	if err := r.tick(context.Background()); err != nil {
		t.Fatalf("tick should not error when publisher fails per-message: %v", err)
	}
	if len(fp.calls) != 0 {
		t.Errorf("no successful publishes expected, got %d", len(fp.calls))
	}
}

func TestToAMQPPriority(t *testing.T) {
	cases := []struct {
		in   int16
		want uint8
	}{{0, 0}, {5, 5}, {255, 255}, {-1, 0}, {300, 255}}
	for _, c := range cases {
		if got := toAMQPPriority(c.in); got != c.want {
			t.Errorf("toAMQPPriority(%d)=%d want %d", c.in, got, c.want)
		}
	}
}
