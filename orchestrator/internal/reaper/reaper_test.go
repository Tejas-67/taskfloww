package reaper

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/Tejas-67/taskfloww/orchestrator/internal/backoff"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/config"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/store"
)

func TestNextFire(t *testing.T) {
	after := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	got, err := NextFire("*/5 * * * *", "UTC", after)
	if err != nil {
		t.Fatal(err)
	}
	want := after.Add(5 * time.Minute)
	if !got.Equal(want) {
		t.Errorf("NextFire = %v, want %v", got, want)
	}
}

func TestNextFireBadCron(t *testing.T) {
	if _, err := NextFire("not a cron", "UTC", time.Now()); err == nil {
		t.Fatal("expected error for bad cron")
	}
}

func TestNextFireBadTimezoneFallsBackToUTC(t *testing.T) {
	// invalid timezone should not error; it falls back to UTC
	if _, err := NextFire("*/5 * * * *", "Not/AZone", time.Now()); err != nil {
		t.Errorf("bad timezone should fall back, got %v", err)
	}
}

type fakeStore struct {
	reapCalls, fireCalls, staleCalls int
}

func (f *fakeStore) ReapExpiredLeases(context.Context, int, func(int) time.Duration) (int, int, error) {
	f.reapCalls++
	return 2, 1, nil
}
func (f *fakeStore) FireDueSchedules(context.Context, int, store.NextFireFunc) (int, error) {
	f.fireCalls++
	return 3, nil
}
func (f *fakeStore) MarkStaleWorkers(context.Context, time.Duration) (int64, error) {
	f.staleCalls++
	return 1, nil
}

func TestTickRunsAllScans(t *testing.T) {
	fs := &fakeStore{}
	r := New(fs, backoff.New(config.Backoff{Strategy: "fixed", BaseSeconds: 1, MaxSeconds: 1}),
		time.Second, 100, 60*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	r.tick(context.Background())
	if fs.reapCalls != 1 || fs.fireCalls != 1 || fs.staleCalls != 1 {
		t.Errorf("expected each scan once, got reap=%d fire=%d stale=%d", fs.reapCalls, fs.fireCalls, fs.staleCalls)
	}
}
