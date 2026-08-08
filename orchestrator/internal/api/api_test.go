package api_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Tejas-67/taskfloww/orchestrator/internal/api"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/domain"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/service"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/store"
)

// fakeSvc implements service.SchedulerService via injectable funcs.
type fakeSvc struct {
	submit   func(context.Context, service.SubmitRequest) (*service.SubmitResult, error)
	get      func(context.Context, string) (*domain.Task, error)
	cancel   func(context.Context, string) (*domain.Task, error)
	listDL   func(context.Context, int, int, bool) ([]domain.DeadLetter, error)
	getDL    func(context.Context, string) (*domain.DeadLetter, error)
	replayDL func(context.Context, string) (*domain.Task, error)
}

func (f *fakeSvc) Submit(ctx context.Context, r service.SubmitRequest) (*service.SubmitResult, error) {
	return f.submit(ctx, r)
}
func (f *fakeSvc) GetTask(ctx context.Context, id string) (*domain.Task, error) {
	return f.get(ctx, id)
}
func (f *fakeSvc) CancelTask(ctx context.Context, id string) (*domain.Task, error) {
	return f.cancel(ctx, id)
}
func (f *fakeSvc) ListDeadLetters(ctx context.Context, limit, offset int, incl bool) ([]domain.DeadLetter, error) {
	return f.listDL(ctx, limit, offset, incl)
}
func (f *fakeSvc) GetDeadLetter(ctx context.Context, id string) (*domain.DeadLetter, error) {
	return f.getDL(ctx, id)
}
func (f *fakeSvc) ReplayDeadLetter(ctx context.Context, id string) (*domain.Task, error) {
	return f.replayDL(ctx, id)
}

const validUUID = "11111111-1111-1111-1111-111111111111"

func do(t *testing.T, svc service.SchedulerService, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	router := api.NewRouter(svc, nil)
	var r *http.Request
	if body != "" {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
	} else {
		r = httptest.NewRequest(method, path, nil)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	return w
}

func TestSubmitTaskCreated(t *testing.T) {
	svc := &fakeSvc{submit: func(_ context.Context, _ service.SubmitRequest) (*service.SubmitResult, error) {
		return &service.SubmitResult{Kind: "task", Task: &domain.Task{ID: validUUID, State: domain.TaskQueued}}, nil
	}}
	w := do(t, svc, http.MethodPost, "/v1/tasks", `{"task_name":"send_email"}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		Kind string       `json:"kind"`
		Task *domain.Task `json:"task"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Kind != "task" || resp.Task == nil || resp.Task.ID != validUUID {
		t.Errorf("unexpected body: %s", w.Body.String())
	}
}

func TestSubmitIdempotentReturns200(t *testing.T) {
	svc := &fakeSvc{submit: func(_ context.Context, _ service.SubmitRequest) (*service.SubmitResult, error) {
		return &service.SubmitResult{Kind: "task", Task: &domain.Task{ID: validUUID}, Existed: true}, nil
	}}
	w := do(t, svc, http.MethodPost, "/v1/tasks", `{"task_name":"send_email","id":"dup"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 for idempotent replay", w.Code)
	}
}

func TestSubmitValidationReturns400(t *testing.T) {
	svc := &fakeSvc{submit: func(_ context.Context, _ service.SubmitRequest) (*service.SubmitResult, error) {
		return nil, &service.ValidationError{Msg: "task_name is required"}
	}}
	w := do(t, svc, http.MethodPost, "/v1/tasks", `{}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
	if !strings.Contains(w.Body.String(), "invalid_request") {
		t.Errorf("expected invalid_request code, got %s", w.Body.String())
	}
}

func TestSubmitMalformedJSON(t *testing.T) {
	svc := &fakeSvc{submit: func(_ context.Context, _ service.SubmitRequest) (*service.SubmitResult, error) {
		t.Fatal("service should not be called on malformed JSON")
		return nil, nil
	}}
	w := do(t, svc, http.MethodPost, "/v1/tasks", `{bad`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestGetTaskOK(t *testing.T) {
	svc := &fakeSvc{get: func(_ context.Context, id string) (*domain.Task, error) {
		return &domain.Task{ID: id, State: domain.TaskRunning}, nil
	}}
	w := do(t, svc, http.MethodGet, "/v1/tasks/"+validUUID, "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}

func TestGetTaskNotFound(t *testing.T) {
	svc := &fakeSvc{get: func(_ context.Context, _ string) (*domain.Task, error) {
		return nil, store.ErrNotFound
	}}
	w := do(t, svc, http.MethodGet, "/v1/tasks/"+validUUID, "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestGetTaskInvalidUUID(t *testing.T) {
	svc := &fakeSvc{get: func(_ context.Context, _ string) (*domain.Task, error) {
		t.Fatal("service should not be called for invalid uuid")
		return nil, nil
	}}
	w := do(t, svc, http.MethodGet, "/v1/tasks/not-a-uuid", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestCancelConflict(t *testing.T) {
	svc := &fakeSvc{cancel: func(_ context.Context, _ string) (*domain.Task, error) {
		return nil, store.ErrConflict
	}}
	w := do(t, svc, http.MethodPost, "/v1/tasks/"+validUUID+"/cancel", "")
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", w.Code)
	}
}

func TestHealthz(t *testing.T) {
	w := do(t, &fakeSvc{}, http.MethodGet, "/healthz", "")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "ok") {
		t.Fatalf("healthz failed: %d %s", w.Code, w.Body.String())
	}
}

func TestListDeadLetters(t *testing.T) {
	svc := &fakeSvc{listDL: func(_ context.Context, _, _ int, _ bool) ([]domain.DeadLetter, error) {
		return []domain.DeadLetter{{ID: validUUID, TaskName: "flaky"}}, nil
	}}
	w := do(t, svc, http.MethodGet, "/v1/dead-letters?limit=10", "")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "flaky") {
		t.Fatalf("list dead-letters failed: %d %s", w.Code, w.Body.String())
	}
}

func TestReplayDeadLetterOK(t *testing.T) {
	svc := &fakeSvc{replayDL: func(_ context.Context, _ string) (*domain.Task, error) {
		return &domain.Task{ID: validUUID, State: domain.TaskQueued}, nil
	}}
	w := do(t, svc, http.MethodPost, "/v1/dead-letters/"+validUUID+"/replay", "")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "replayed") {
		t.Fatalf("replay failed: %d %s", w.Code, w.Body.String())
	}
}

func TestReplayDeadLetterAlreadyReplayed(t *testing.T) {
	svc := &fakeSvc{replayDL: func(_ context.Context, _ string) (*domain.Task, error) {
		return nil, store.ErrConflict
	}}
	w := do(t, svc, http.MethodPost, "/v1/dead-letters/"+validUUID+"/replay", "")
	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", w.Code)
	}
}
