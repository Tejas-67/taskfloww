// Package api exposes the SchedulerService over REST (chi). It is a thin
// transport layer: decode → call service → encode, mapping domain errors to
// HTTP status codes. A gRPC layer can wrap the same SchedulerService later
// (ADR-0003).
package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"

	"github.com/Tejas-67/taskfloww/orchestrator/internal/domain"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/service"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/store"
)

const maxBodyBytes = 1 << 20 // 1 MiB

// Handler wires the SchedulerService into HTTP routes.
type Handler struct {
	svc    service.SchedulerService
	logger *slog.Logger
}

// NewRouter builds the chi router (health + /v1 task routes).
func NewRouter(svc service.SchedulerService, logger *slog.Logger) *chi.Mux {
	h := &Handler{svc: svc, logger: logger}
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Recoverer)
	r.Use(requestLogger(logger))

	r.Get("/healthz", h.health)
	r.Route("/v1", func(r chi.Router) {
		r.Post("/tasks", h.submitTask)
		r.Get("/tasks/{id}", h.getTask)
		r.Post("/tasks/{id}/cancel", h.cancelTask)

		r.Get("/dead-letters", h.listDeadLetters)
		r.Get("/dead-letters/{id}", h.getDeadLetter)
		r.Post("/dead-letters/{id}/replay", h.replayDeadLetter)
	})
	return r
}

// --- DTOs ------------------------------------------------------------------

type submitRequestDTO struct {
	ID            string          `json:"id"`
	TaskName      string          `json:"task_name"`
	Payload       json.RawMessage `json:"payload"`
	Priority      *int16          `json:"priority"`
	MaxRetries    *int            `json:"max_retries"`
	ExecutionType string          `json:"execution_type"`
	RunAt         *time.Time      `json:"run_at"`
	DelaySeconds  *int            `json:"delay_seconds"`
	Cron          string          `json:"cron"`
	Timezone      string          `json:"timezone"`
	ScheduleName  string          `json:"schedule_name"`
}

type submitResponseDTO struct {
	Kind     string           `json:"kind"`
	Task     *domain.Task     `json:"task,omitempty"`
	Schedule *domain.Schedule `json:"schedule,omitempty"`
}

type errorDTO struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// --- handlers --------------------------------------------------------------

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) submitTask(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	var dto submitRequestDTO
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&dto); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "malformed JSON body: "+err.Error())
		return
	}

	req := service.SubmitRequest{
		IdempotencyKey: dto.ID,
		TaskName:       dto.TaskName,
		Payload:        dto.Payload,
		Priority:       dto.Priority,
		MaxRetries:     dto.MaxRetries,
		ExecutionType:  domain.ExecutionType(dto.ExecutionType),
		RunAt:          dto.RunAt,
		DelaySeconds:   dto.DelaySeconds,
		Cron:           dto.Cron,
		Timezone:       dto.Timezone,
		ScheduleName:   dto.ScheduleName,
	}

	res, err := h.svc.Submit(r.Context(), req)
	if err != nil {
		h.mapError(w, err)
		return
	}

	status := http.StatusCreated
	if res.Existed {
		status = http.StatusOK // idempotent replay of a prior submission
	}
	writeJSON(w, status, submitResponseDTO{Kind: res.Kind, Task: res.Task, Schedule: res.Schedule})
}

func (h *Handler) getTask(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	task, err := h.svc.GetTask(r.Context(), id)
	if err != nil {
		h.mapError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, task)
}

func (h *Handler) cancelTask(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	task, err := h.svc.CancelTask(r.Context(), id)
	if err != nil {
		h.mapError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, task)
}

func (h *Handler) listDeadLetters(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := atoiDefault(q.Get("limit"), 0)
	offset := atoiDefault(q.Get("offset"), 0)
	includeReplayed := q.Get("include_replayed") == "true"
	items, err := h.svc.ListDeadLetters(r.Context(), limit, offset, includeReplayed)
	if err != nil {
		h.mapError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"dead_letters": items, "count": len(items)})
}

func (h *Handler) getDeadLetter(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	dl, err := h.svc.GetDeadLetter(r.Context(), id)
	if err != nil {
		h.mapError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, dl)
}

func (h *Handler) replayDeadLetter(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	task, err := h.svc.ReplayDeadLetter(r.Context(), id)
	if err != nil {
		h.mapError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"replayed": true, "task": task})
}

// --- helpers ---------------------------------------------------------------

// parseID extracts and validates the {id} path param as a UUID.
func parseID(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := chi.URLParam(r, "id")
	if _, err := uuid.Parse(id); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "id must be a UUID")
		return "", false
	}
	return id, true
}

// atoiDefault parses s as an int, returning def on empty/invalid input.
func atoiDefault(s string, def int) int {
	if s == "" {
		return def
	}
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return def
}

// mapError translates domain/store errors into HTTP responses.
func (h *Handler) mapError(w http.ResponseWriter, err error) {
	var ve *service.ValidationError
	switch {
	case errors.As(err, &ve):
		writeError(w, http.StatusBadRequest, "invalid_request", ve.Msg)
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "task not found")
	case errors.Is(err, store.ErrConflict):
		writeError(w, http.StatusConflict, "conflict", "task cannot transition in its current state")
	case errors.Is(err, store.ErrDuplicate):
		writeError(w, http.StatusConflict, "duplicate", "a resource with that identifier already exists")
	default:
		if h.logger != nil {
			h.logger.Error("internal error handling request", "error", err)
		}
		writeError(w, http.StatusInternalServerError, "internal", "internal server error")
	}
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	var e errorDTO
	e.Error.Code = code
	e.Error.Message = msg
	writeJSON(w, status, e)
}

// requestLogger logs each request with structured fields.
func requestLogger(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if logger == nil {
				next.ServeHTTP(w, r)
				return
			}
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			start := time.Now()
			next.ServeHTTP(ww, r)
			logger.Info("http request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", ww.Status(),
				"bytes", ww.BytesWritten(),
				"duration_ms", time.Since(start).Milliseconds(),
				"request_id", middleware.GetReqID(r.Context()),
			)
		})
	}
}
