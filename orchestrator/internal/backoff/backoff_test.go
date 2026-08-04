package backoff

import (
	"testing"
	"time"

	"github.com/Tejas-67/taskfloww/orchestrator/internal/config"
)

func TestExponentialNoJitter(t *testing.T) {
	p := &Policy{Backoff: config.Backoff{
		Strategy: "exponential", BaseSeconds: 2, Multiplier: 2, MaxSeconds: 300, Jitter: false,
	}}
	cases := map[int]time.Duration{
		1: 2 * time.Second,  // 2 * 2^0
		2: 4 * time.Second,  // 2 * 2^1
		3: 8 * time.Second,  // 2 * 2^2
		4: 16 * time.Second, // 2 * 2^3
	}
	for attempt, want := range cases {
		if got := p.Next(attempt); got != want {
			t.Errorf("Next(%d) = %v, want %v", attempt, got, want)
		}
	}
}

func TestCappedAtMax(t *testing.T) {
	p := &Policy{Backoff: config.Backoff{
		Strategy: "exponential", BaseSeconds: 10, Multiplier: 10, MaxSeconds: 30, Jitter: false,
	}}
	if got := p.Next(5); got != 30*time.Second {
		t.Errorf("expected cap at 30s, got %v", got)
	}
}

func TestFixed(t *testing.T) {
	p := &Policy{Backoff: config.Backoff{Strategy: "fixed", BaseSeconds: 5, MaxSeconds: 300}}
	for _, a := range []int{1, 3, 9} {
		if got := p.Next(a); got != 5*time.Second {
			t.Errorf("fixed Next(%d) = %v, want 5s", a, got)
		}
	}
}

func TestJitterBounds(t *testing.T) {
	base := config.Backoff{Strategy: "exponential", BaseSeconds: 8, Multiplier: 2, MaxSeconds: 300, Jitter: true}
	// r=0 -> lower bound (secs/2); r~1 -> upper bound (secs)
	low := (&Policy{Backoff: base, Rand: func() float64 { return 0 }}).Next(1)
	high := (&Policy{Backoff: base, Rand: func() float64 { return 0.999999 }}).Next(1)
	if low != 4*time.Second {
		t.Errorf("jitter lower bound = %v, want 4s", low)
	}
	if high <= 4*time.Second || high > 8*time.Second {
		t.Errorf("jitter upper bound = %v, want (4s,8s]", high)
	}
}
