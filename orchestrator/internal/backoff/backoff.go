// Package backoff computes retry delays from the configured policy
// (exponential or fixed, with optional jitter). Used by the result consumer and
// the reaper to schedule the next attempt of a failed task.
package backoff

import (
	"math"
	"math/rand"
	"time"

	"github.com/Tejas-67/taskfloww/orchestrator/internal/config"
)

// Policy computes retry delays. Rand returns a value in [0,1) and is injectable
// for deterministic tests.
type Policy struct {
	Backoff config.Backoff
	Rand    func() float64
}

// New builds a Policy from config using the default RNG.
func New(b config.Backoff) *Policy {
	return &Policy{Backoff: b, Rand: rand.Float64}
}

// Next returns the delay before the next attempt, given the attempt number that
// just failed (1-based). With jitter, the result is in [delay/2, delay).
func (p *Policy) Next(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	secs := p.Backoff.BaseSeconds
	if p.Backoff.Strategy != "fixed" { // exponential
		secs = p.Backoff.BaseSeconds * math.Pow(p.Backoff.Multiplier, float64(attempt-1))
	}
	if p.Backoff.MaxSeconds > 0 && secs > p.Backoff.MaxSeconds {
		secs = p.Backoff.MaxSeconds
	}
	if p.Backoff.Jitter {
		r := 0.5
		if p.Rand != nil {
			r = p.Rand()
		}
		secs = secs*0.5 + secs*0.5*r // [secs/2, secs)
	}
	return time.Duration(secs * float64(time.Second))
}
