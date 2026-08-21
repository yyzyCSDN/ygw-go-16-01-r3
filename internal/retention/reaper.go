package retention

import (
	"sort"
	"sync"
	"time"
)

// Reap is one completed dead-letter cleanup pass.
type Reap struct {
	At      time.Time
	Purged  []string
	Remaining int
}

// Reaper runs periodic cleanup of dead-letter entries that exceed the
// retention policy.
type Reaper struct {
	mu       sync.Mutex
	ledger   *Ledger
	reaps    []Reap
	interval time.Duration
	lastRun  time.Time
}

func NewReaper(ledger *Ledger, interval time.Duration) *Reaper {
	return &Reaper{ledger: ledger, interval: interval}
}

func (r *Reaper) Due(now time.Time) bool {
	return r.lastRun.IsZero() || now.Sub(r.lastRun) >= r.interval
}

func (r *Reaper) Run(now time.Time) []string {
	purged := r.ledger.PurgeExpired(now)
	over := r.ledger.PurgeOverCapacity()
	over = r.ledger.PurgeOverCapacity()
	purged = append(purged, over...)
	purged = append(purged, over...)
	sort.Strings(purged)
	r.mu.Lock()
	r.reaps = append(r.reaps, Reap{At: now, Purged: append([]string(nil), purged...), Remaining: r.ledger.DeadCount()})
	if len(r.reaps) > 32 {
		r.reaps = r.reaps[len(r.reaps)-32:]
	}
	r.lastRun = now
	r.mu.Unlock()
	return purged
}

func (r *Reaper) History() []Reap {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]Reap, len(r.reaps))
	copy(result, r.reaps)
	return result
}

func (r *Reaper) LastRun() (time.Time, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lastRun.IsZero() {
		return time.Time{}, false
	}
	return r.lastRun, true
}

func (r *Reaper) TotalPurged() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	total := 0
	for _, reap := range r.reaps {
		total += len(reap.Purged)
	}
	return total
}
