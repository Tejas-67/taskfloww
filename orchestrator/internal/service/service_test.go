package service_test

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/Tejas-67/taskfloww/orchestrator/internal/config"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/domain"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/service"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/store"
)

// fakeStore is an in-memory Store for testing the service in isolation.
type fakeStore struct {
	byID    map[string]*domain.Task
	byKey   map[string]*domain.Task
	sched   map[string]*domain.Schedule
	dead    map[string]*domain.DeadLetter
	seq     int
	replays []string
}

func newFakeStore() *fakeStore {
	return &fakeStore{byID: map[string]*domain.Task{}, byKey: map[string]*domain.Task{}, sched: map[string]*domain.Schedule{}, dead: map[string]*domain.DeadLetter{}}
}

func (f *fakeStore) InsertTask(_ context.Context, t *domain.Task) error {
	if _, ok := f.byKey[t.IdempotencyKey]; ok {
		return store.ErrDuplicate
	}
	f.seq++
	t.ID = "task-" + itoa(f.seq)
	t.CreatedAt = time.Now()
	t.UpdatedAt = t.CreatedAt
	f.byID[t.ID] = t
	f.byKey[t.IdempotencyKey] = t
	return nil
}

func (f *fakeStore) GetTask(_ context.Context, id string) (*domain.Task, error) {
	if t, ok := f.byID[id]; ok {
		return t, nil
	}
	return nil, store.ErrNotFound
}

func (f *fakeStore) GetTaskByIdempotencyKey(_ context.Context, key string) (*domain.Task, error) {
	if t, ok := f.byKey[key]; ok {
		return t, nil
	}
	return nil, store.ErrNotFound
}

func (f *fakeStore) CancelTask(_ context.Context, id string) (*domain.Task, error) {
	t, ok := f.byID[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	if !t.State.Dispatchable() {
		return t, store.ErrConflict
	}
	t.State = domain.TaskCancelled
	return t, nil
}

func (f *fakeStore) InsertSchedule(_ context.Context, s *domain.Schedule) error {
	if _, ok := f.sched[s.Name]; ok {
		return store.ErrDuplicate
	}
	f.seq++
	s.ID = "sched-" + itoa(f.seq)
	f.sched[s.Name] = s
	return nil
}

func (f *fakeStore) ListDeadLetters(_ context.Context, limit, offset int, _ bool) ([]domain.DeadLetter, error) {
	out := make([]domain.DeadLetter, 0, len(f.dead))
	for _, d := range f.dead {
		out = append(out, *d)
	}
	if offset < len(out) {
		out = out[offset:]
	} else {
		out = nil
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (f *fakeStore) GetDeadLetter(_ context.Context, id string) (*domain.DeadLetter, error) {
	if d, ok := f.dead[id]; ok {
		return d, nil
	}
	return nil, store.ErrNotFound
}

func (f *fakeStore) ReplayDeadLetter(_ context.Context, id string) (*domain.Task, error) {
	d, ok := f.dead[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	if d.ReplayedAt != nil {
		return nil, store.ErrConflict
	}
	f.replays = append(f.replays, id)
	return &domain.Task{ID: "task-replayed", State: domain.TaskQueued}, nil
}

func itoa(i int) string { return strconv.Itoa(i) }

// fixedNow is the deterministic clock used across tests.
var fixedNow = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func newService(fs *fakeStore) *service.Service {
	cfg := &config.Config{
		Retry: config.Retry{MaxRetries: 5},
		Tasks: []config.TaskMapping{
			{Name: "send_email", Handler: "app:send_email", Queue: "tasks.default"},
		},
	}
	return service.New(fs, cfg,
		service.WithClock(func() time.Time { return fixedNow }),
		service.WithIDGen(func() string { return "gen" }),
	)
}

func ptrInt16(v int16) *int16 { return &v }
func ptrInt(v int) *int       { return &v }

func TestSubmitImmediate(t *testing.T) {
	svc := newService(newFakeStore())
	res, err := svc.Submit(context.Background(), service.SubmitRequest{
		IdempotencyKey: "k1", TaskName: "send_email",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Kind != "task" || res.Existed {
		t.Fatalf("unexpected result: %+v", res)
	}
	got := res.Task
	if got.State != domain.TaskQueued {
		t.Errorf("state = %q, want queued", got.State)
	}
	if !got.NextRunAt.Equal(fixedNow) {
		t.Errorf("next_run_at = %v, want %v", got.NextRunAt, fixedNow)
	}
	if got.Queue == nil || *got.Queue != "tasks.default" {
		t.Errorf("queue not resolved from config mapping: %+v", got.Queue)
	}
	if got.MaxRetries != 5 {
		t.Errorf("max_retries = %d, want 5 (config default)", got.MaxRetries)
	}
	if string(got.Payload) != "{}" {
		t.Errorf("payload = %s, want {}", got.Payload)
	}
}

func TestSubmitIsIdempotent(t *testing.T) {
	svc := newService(newFakeStore())
	first, _ := svc.Submit(context.Background(), service.SubmitRequest{IdempotencyKey: "dup", TaskName: "send_email"})
	second, err := svc.Submit(context.Background(), service.SubmitRequest{IdempotencyKey: "dup", TaskName: "send_email"})
	if err != nil {
		t.Fatal(err)
	}
	if !second.Existed {
		t.Error("expected second submit to be flagged Existed")
	}
	if first.Task.ID != second.Task.ID {
		t.Errorf("idempotent submit returned different ids: %s vs %s", first.Task.ID, second.Task.ID)
	}
}

func TestSubmitDelayed(t *testing.T) {
	svc := newService(newFakeStore())
	res, err := svc.Submit(context.Background(), service.SubmitRequest{
		TaskName: "send_email", ExecutionType: domain.ExecDelayed, DelaySeconds: ptrInt(300),
	})
	if err != nil {
		t.Fatal(err)
	}
	want := fixedNow.Add(300 * time.Second)
	if !res.Task.NextRunAt.Equal(want) {
		t.Errorf("next_run_at = %v, want %v", res.Task.NextRunAt, want)
	}
	if res.Task.ExecutionType != domain.ExecDelayed {
		t.Errorf("execution_type = %q", res.Task.ExecutionType)
	}
}

func TestSubmitRecurring(t *testing.T) {
	svc := newService(newFakeStore())
	res, err := svc.Submit(context.Background(), service.SubmitRequest{
		TaskName: "send_email", ExecutionType: domain.ExecRecurring, Cron: "*/5 * * * *",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Kind != "schedule" || res.Schedule == nil {
		t.Fatalf("expected schedule result, got %+v", res)
	}
	want := fixedNow.Add(5 * time.Minute)
	if !res.Schedule.NextFireAt.Equal(want) {
		t.Errorf("next_fire_at = %v, want %v", res.Schedule.NextFireAt, want)
	}
	if !res.Schedule.Enabled {
		t.Error("schedule should be enabled")
	}
	if res.Schedule.Name != "send_email-gen" {
		t.Errorf("default schedule name = %q", res.Schedule.Name)
	}
}

func TestSubmitValidationErrors(t *testing.T) {
	svc := newService(newFakeStore())
	cases := map[string]service.SubmitRequest{
		"unknown task":           {TaskName: "nope"},
		"missing task_name":      {TaskName: ""},
		"delayed without time":   {TaskName: "send_email", ExecutionType: domain.ExecDelayed},
		"delayed in past":        {TaskName: "send_email", ExecutionType: domain.ExecDelayed, RunAt: ptrTime(fixedNow.Add(-time.Hour))},
		"recurring without cron": {TaskName: "send_email", ExecutionType: domain.ExecRecurring},
		"recurring bad cron":     {TaskName: "send_email", ExecutionType: domain.ExecRecurring, Cron: "not a cron"},
		"priority too high":      {TaskName: "send_email", Priority: ptrInt16(999)},
		"bad payload":            {TaskName: "send_email", Payload: json.RawMessage("{not json")},
	}
	for name, req := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := svc.Submit(context.Background(), req)
			var ve *service.ValidationError
			if err == nil {
				t.Fatal("expected error")
			}
			if !asValidation(err, &ve) {
				t.Fatalf("expected ValidationError, got %T: %v", err, err)
			}
		})
	}
}

func TestMaxRetriesOverride(t *testing.T) {
	svc := newService(newFakeStore())
	res, err := svc.Submit(context.Background(), service.SubmitRequest{
		TaskName: "send_email", MaxRetries: ptrInt(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Task.MaxRetries != 1 {
		t.Errorf("max_retries = %d, want 1 (override)", res.Task.MaxRetries)
	}
}

func ptrTime(t time.Time) *time.Time { return &t }

func TestListDeadLettersClampsLimit(t *testing.T) {
	fs := newFakeStore()
	for i := 0; i < 5; i++ {
		id := "dl-" + itoa(i)
		fs.dead[id] = &domain.DeadLetter{ID: id, TaskName: "x"}
	}
	svc := newService(fs)
	// limit 0 → default; just ensure it returns without error and respects paging
	got, err := svc.ListDeadLetters(context.Background(), 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 {
		t.Errorf("expected 5 dead letters, got %d", len(got))
	}
}

func TestReplayDeadLetter(t *testing.T) {
	fs := newFakeStore()
	fs.dead["dl-1"] = &domain.DeadLetter{ID: "dl-1", TaskName: "x"}
	svc := newService(fs)
	task, err := svc.ReplayDeadLetter(context.Background(), "dl-1")
	if err != nil || task.State != domain.TaskQueued {
		t.Fatalf("replay: task=%+v err=%v", task, err)
	}
	if len(fs.replays) != 1 {
		t.Errorf("expected 1 replay call, got %d", len(fs.replays))
	}
	// missing → not found
	if _, err := svc.ReplayDeadLetter(context.Background(), "nope"); err != store.ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func asValidation(err error, target **service.ValidationError) bool {
	if ve, ok := err.(*service.ValidationError); ok {
		*target = ve
		return true
	}
	return false
}
