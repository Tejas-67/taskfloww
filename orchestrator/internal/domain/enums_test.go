package domain

import "testing"

func TestTaskStateValid(t *testing.T) {
	valid := []TaskState{
		TaskQueued, TaskDispatching, TaskRunning, TaskRetrying,
		TaskCompleted, TaskFailed, TaskDead, TaskCancelled,
	}
	for _, s := range valid {
		if !s.Valid() {
			t.Errorf("expected %q to be valid", s)
		}
	}
	if TaskState("bogus").Valid() {
		t.Error("expected bogus state to be invalid")
	}
}

func TestTaskStateTerminal(t *testing.T) {
	cases := map[TaskState]bool{
		TaskQueued:      false,
		TaskDispatching: false,
		TaskRunning:     false,
		TaskRetrying:    false,
		TaskCompleted:   true,
		TaskFailed:      true,
		TaskDead:        true,
		TaskCancelled:   true,
	}
	for s, want := range cases {
		if got := s.Terminal(); got != want {
			t.Errorf("%q.Terminal() = %v, want %v", s, got, want)
		}
	}
}

// Dispatchable must match the idx_tasks_due partial-index predicate
// (state IN ('queued','retrying')). If these drift, the dispatcher and its
// index disagree — so this test guards that invariant.
func TestTaskStateDispatchable(t *testing.T) {
	for _, s := range []TaskState{TaskQueued, TaskRetrying} {
		if !s.Dispatchable() {
			t.Errorf("expected %q to be dispatchable", s)
		}
	}
	for _, s := range []TaskState{TaskRunning, TaskCompleted, TaskDead} {
		if s.Dispatchable() {
			t.Errorf("expected %q NOT to be dispatchable", s)
		}
	}
}

// InFlight must match the idx_tasks_lease_expiry partial-index predicate
// (state IN ('dispatching','running')).
func TestTaskStateInFlight(t *testing.T) {
	for _, s := range []TaskState{TaskDispatching, TaskRunning} {
		if !s.InFlight() {
			t.Errorf("expected %q to be in-flight", s)
		}
	}
	if TaskQueued.InFlight() {
		t.Error("queued must not be in-flight")
	}
}

func TestOtherEnumsValid(t *testing.T) {
	if !ExecImmediate.Valid() || !ExecDelayed.Valid() || !ExecRecurring.Valid() {
		t.Error("execution types should be valid")
	}
	if ExecutionType("nope").Valid() {
		t.Error("unknown execution type should be invalid")
	}
	if !ExecutionSucceeded.Valid() || !WorkerAlive.Valid() {
		t.Error("execution state / worker status should be valid")
	}
}

func TestRetriesRemaining(t *testing.T) {
	cases := []struct {
		max, attempt, want int
	}{
		{5, 0, 5},
		{5, 3, 2},
		{3, 3, 0},
		{3, 5, 0}, // never negative
	}
	for _, c := range cases {
		task := &Task{MaxRetries: c.max, Attempt: c.attempt}
		if got := task.RetriesRemaining(); got != c.want {
			t.Errorf("RetriesRemaining(max=%d,attempt=%d) = %d, want %d", c.max, c.attempt, got, c.want)
		}
	}
}
