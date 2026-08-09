package metrics

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestHandlerExposesMetrics(t *testing.T) {
	TasksDispatched.Add(3)
	TaskResults.WithLabelValues("completed").Inc()
	WorkersAlive.Set(2)
	HTTPRequestDuration.WithLabelValues("GET", "/v1/tasks", "201").Observe(0.01)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	body, _ := io.ReadAll(w.Body)
	out := string(body)
	for _, name := range []string{
		"taskfloww_orchestrator_tasks_dispatched_total",
		"taskfloww_orchestrator_task_results_total",
		"taskfloww_orchestrator_workers_alive",
		"taskfloww_orchestrator_http_request_duration_seconds",
	} {
		if !strings.Contains(out, name) {
			t.Errorf("expected metric %q in output", name)
		}
	}
}

type fakeStats struct {
	byState map[string]int
	alive   int
	pending int
}

func (f fakeStats) CountActiveTasksByState(context.Context) (map[string]int, error) {
	return f.byState, nil
}
func (f fakeStats) CountAliveWorkers(context.Context) (int, error)  { return f.alive, nil }
func (f fakeStats) CountPendingOutbox(context.Context) (int, error) { return f.pending, nil }

func TestCollectorSetsGauges(t *testing.T) {
	c := NewCollector(fakeStats{
		byState: map[string]int{"queued": 5, "running": 2},
		alive:   4,
		pending: 7,
	}, 0, slog.New(slog.NewTextHandler(io.Discard, nil)))

	c.sample(context.Background())

	if got := testutil.ToFloat64(TasksActive.WithLabelValues("queued")); got != 5 {
		t.Errorf("queued gauge = %v, want 5", got)
	}
	// a state absent from the store report must be reset to 0, not stale
	if got := testutil.ToFloat64(TasksActive.WithLabelValues("dispatching")); got != 0 {
		t.Errorf("dispatching gauge = %v, want 0", got)
	}
	if got := testutil.ToFloat64(WorkersAlive); got != 4 {
		t.Errorf("workers_alive = %v, want 4", got)
	}
	if got := testutil.ToFloat64(OutboxPending); got != 7 {
		t.Errorf("outbox_pending = %v, want 7", got)
	}
}
