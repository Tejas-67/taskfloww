// Package service holds TaskFloww's transport-agnostic business logic. The
// SchedulerService interface is implemented here and consumed by the API layer
// (and, later, a gRPC layer) so submission/validation logic lives in one place
// (ADR-0003).
package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"

	"github.com/Tejas-67/taskfloww/orchestrator/internal/config"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/domain"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/store"
)

// Store is the persistence surface the scheduler needs (a consumer-defined
// interface; store.Postgres satisfies it).
type Store interface {
	InsertTask(ctx context.Context, t *domain.Task) error
	GetTask(ctx context.Context, id string) (*domain.Task, error)
	GetTaskByIdempotencyKey(ctx context.Context, key string) (*domain.Task, error)
	CancelTask(ctx context.Context, id string) (*domain.Task, error)
	InsertSchedule(ctx context.Context, s *domain.Schedule) error
	ListDeadLetters(ctx context.Context, limit, offset int, includeReplayed bool) ([]domain.DeadLetter, error)
	GetDeadLetter(ctx context.Context, id string) (*domain.DeadLetter, error)
	ReplayDeadLetter(ctx context.Context, id string) (*domain.Task, error)
}

// SchedulerService is the core API surface (transport-agnostic).
type SchedulerService interface {
	Submit(ctx context.Context, req SubmitRequest) (*SubmitResult, error)
	GetTask(ctx context.Context, id string) (*domain.Task, error)
	CancelTask(ctx context.Context, id string) (*domain.Task, error)
	ListDeadLetters(ctx context.Context, limit, offset int, includeReplayed bool) ([]domain.DeadLetter, error)
	GetDeadLetter(ctx context.Context, id string) (*domain.DeadLetter, error)
	ReplayDeadLetter(ctx context.Context, id string) (*domain.Task, error)
}

// ValidationError signals a bad request (mapped to HTTP 400).
type ValidationError struct{ Msg string }

func (e *ValidationError) Error() string { return e.Msg }

func invalid(format string, args ...interface{}) *ValidationError {
	return &ValidationError{Msg: fmt.Sprintf(format, args...)}
}

// SubmitRequest is a transport-agnostic task submission.
type SubmitRequest struct {
	IdempotencyKey string // client "unique id"; generated if empty
	TaskName       string
	Payload        json.RawMessage
	Priority       *int16
	MaxRetries     *int
	ExecutionType  domain.ExecutionType // default: immediate

	// delayed:
	RunAt        *time.Time
	DelaySeconds *int

	// recurring:
	Cron         string
	Timezone     string // default: UTC
	ScheduleName string // default: "<task_name>-<uuid>"
}

// SubmitResult carries the created task (immediate/delayed) or schedule
// (recurring). Existed is true when an idempotent duplicate returned the
// pre-existing task.
type SubmitResult struct {
	Kind     string // "task" | "schedule"
	Task     *domain.Task
	Schedule *domain.Schedule
	Existed  bool
}

// Service implements SchedulerService.
type Service struct {
	store Store
	cfg   *config.Config
	now   func() time.Time
	newID func() string
}

// Option customizes a Service (used mainly in tests).
type Option func(*Service)

// WithClock overrides the time source.
func WithClock(fn func() time.Time) Option { return func(s *Service) { s.now = fn } }

// WithIDGen overrides the id generator.
func WithIDGen(fn func() string) Option { return func(s *Service) { s.newID = fn } }

// New constructs a Service.
func New(st Store, cfg *config.Config, opts ...Option) *Service {
	s := &Service{
		store: st,
		cfg:   cfg,
		now:   func() time.Time { return time.Now().UTC() },
		newID: func() string { return uuid.NewString() },
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

var _ SchedulerService = (*Service)(nil)

// Submit validates a submission and persists a task or a recurring schedule.
func (s *Service) Submit(ctx context.Context, req SubmitRequest) (*SubmitResult, error) {
	if req.TaskName == "" {
		return nil, invalid("task_name is required")
	}
	mapping, ok := s.cfg.TaskByName(req.TaskName)
	if !ok {
		return nil, invalid("unknown task_name %q (add it to the config tasks map)", req.TaskName)
	}

	et := req.ExecutionType
	if et == "" {
		et = domain.ExecImmediate
	}
	if !et.Valid() {
		return nil, invalid("execution_type %q must be immediate|delayed|recurring", et)
	}

	priority := int16(0)
	if req.Priority != nil {
		priority = *req.Priority
		if priority < 0 || priority > 255 {
			return nil, invalid("priority must be 0..255 (got %d)", priority)
		}
	}

	maxRetries := mapping.EffectiveMaxRetries(s.cfg.Retry.MaxRetries)
	if req.MaxRetries != nil {
		if *req.MaxRetries < 0 {
			return nil, invalid("max_retries must be >= 0 (got %d)", *req.MaxRetries)
		}
		maxRetries = *req.MaxRetries
	}

	payload := req.Payload
	if len(payload) == 0 {
		payload = json.RawMessage("{}")
	} else if !json.Valid(payload) {
		return nil, invalid("payload must be valid JSON")
	}

	var queue *string
	if mapping.Queue != "" {
		q := mapping.Queue
		queue = &q
	}

	now := s.now().UTC()

	switch et {
	case domain.ExecImmediate:
		return s.submitTask(ctx, &domain.Task{
			IdempotencyKey: s.keyOrGen(req.IdempotencyKey),
			TaskName:       req.TaskName,
			Payload:        payload,
			State:          domain.TaskQueued,
			ExecutionType:  domain.ExecImmediate,
			Priority:       priority,
			MaxRetries:     maxRetries,
			NextRunAt:      now,
			Queue:          queue,
		})

	case domain.ExecDelayed:
		runAt, err := s.resolveRunAt(req, now)
		if err != nil {
			return nil, err
		}
		return s.submitTask(ctx, &domain.Task{
			IdempotencyKey: s.keyOrGen(req.IdempotencyKey),
			TaskName:       req.TaskName,
			Payload:        payload,
			State:          domain.TaskQueued,
			ExecutionType:  domain.ExecDelayed,
			Priority:       priority,
			MaxRetries:     maxRetries,
			NextRunAt:      runAt,
			Queue:          queue,
		})

	case domain.ExecRecurring:
		return s.submitSchedule(ctx, req, payload, priority, maxRetries, queue, now)
	}
	return nil, invalid("unsupported execution_type %q", et)
}

// submitTask inserts a task, returning the existing one idempotently on a
// duplicate idempotency_key.
func (s *Service) submitTask(ctx context.Context, t *domain.Task) (*SubmitResult, error) {
	err := s.store.InsertTask(ctx, t)
	if errors.Is(err, store.ErrDuplicate) {
		existing, gerr := s.store.GetTaskByIdempotencyKey(ctx, t.IdempotencyKey)
		if gerr != nil {
			return nil, gerr
		}
		return &SubmitResult{Kind: "task", Task: existing, Existed: true}, nil
	}
	if err != nil {
		return nil, err
	}
	return &SubmitResult{Kind: "task", Task: t}, nil
}

func (s *Service) resolveRunAt(req SubmitRequest, now time.Time) (time.Time, error) {
	switch {
	case req.RunAt != nil:
		runAt := req.RunAt.UTC()
		if !runAt.After(now) {
			return time.Time{}, invalid("run_at must be in the future")
		}
		return runAt, nil
	case req.DelaySeconds != nil:
		if *req.DelaySeconds <= 0 {
			return time.Time{}, invalid("delay_seconds must be > 0")
		}
		return now.Add(time.Duration(*req.DelaySeconds) * time.Second), nil
	default:
		return time.Time{}, invalid("delayed tasks require run_at or delay_seconds")
	}
}

func (s *Service) submitSchedule(
	ctx context.Context, req SubmitRequest, payload json.RawMessage,
	priority int16, maxRetries int, queue *string, now time.Time,
) (*SubmitResult, error) {
	if req.Cron == "" {
		return nil, invalid("recurring tasks require a cron expression")
	}
	tz := req.Timezone
	if tz == "" {
		tz = "UTC"
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return nil, invalid("invalid timezone %q", tz)
	}
	sched, err := cron.ParseStandard(req.Cron)
	if err != nil {
		return nil, invalid("invalid cron %q: %v", req.Cron, err)
	}
	next := sched.Next(now.In(loc)).UTC()

	name := req.ScheduleName
	if name == "" {
		name = fmt.Sprintf("%s-%s", req.TaskName, s.newID())
	}
	schedule := &domain.Schedule{
		Name:       name,
		TaskName:   req.TaskName,
		Payload:    payload,
		CronExpr:   req.Cron,
		Timezone:   tz,
		Priority:   priority,
		MaxRetries: maxRetries,
		Queue:      queue,
		Enabled:    true,
		NextFireAt: next,
	}
	if err := s.store.InsertSchedule(ctx, schedule); err != nil {
		return nil, err
	}
	return &SubmitResult{Kind: "schedule", Schedule: schedule}, nil
}

// GetTask returns a task by id.
func (s *Service) GetTask(ctx context.Context, id string) (*domain.Task, error) {
	return s.store.GetTask(ctx, id)
}

// CancelTask cancels a queued/retrying task.
func (s *Service) CancelTask(ctx context.Context, id string) (*domain.Task, error) {
	return s.store.CancelTask(ctx, id)
}

// Dead-letter list paging bounds.
const (
	defaultDLQLimit = 50
	maxDLQLimit     = 200
)

// ListDeadLetters returns dead letters (newest first), clamping paging bounds.
func (s *Service) ListDeadLetters(ctx context.Context, limit, offset int, includeReplayed bool) ([]domain.DeadLetter, error) {
	if limit <= 0 {
		limit = defaultDLQLimit
	}
	if limit > maxDLQLimit {
		limit = maxDLQLimit
	}
	if offset < 0 {
		offset = 0
	}
	return s.store.ListDeadLetters(ctx, limit, offset, includeReplayed)
}

// GetDeadLetter returns a dead letter by id.
func (s *Service) GetDeadLetter(ctx context.Context, id string) (*domain.DeadLetter, error) {
	return s.store.GetDeadLetter(ctx, id)
}

// ReplayDeadLetter re-queues a dead task for another run.
func (s *Service) ReplayDeadLetter(ctx context.Context, id string) (*domain.Task, error) {
	return s.store.ReplayDeadLetter(ctx, id)
}

func (s *Service) keyOrGen(key string) string {
	if key != "" {
		return key
	}
	return s.newID()
}
