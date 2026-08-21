package expiry

import (
	"sort"
	"sync"
	"time"

	"example.com/relaydock/internal/core"
)

// Sweep is one completed expiry pass.
type Sweep struct {
	At        time.Time
	Expired   []string
	Remaining int
}

// Sweeper runs periodic expiry passes over the event set and keeps a bounded
// record of the sweeps that have run.
type Sweeper struct {
	mu       sync.Mutex
	scanner  *Scanner
	sweeps   []Sweep
	interval time.Duration
	lastRun  time.Time
}

func NewSweeper(scanner *Scanner, interval time.Duration) *Sweeper {
	return &Sweeper{scanner: scanner, interval: interval}
}

func (s *Sweeper) Due(now time.Time) bool {
	return s.lastRun.IsZero() || now.Sub(s.lastRun) >= s.interval
}

func (s *Sweeper) Run(events []core.Event, now time.Time, limit int) []string {
	// A limit of zero means the caller requested no scanning this pass, and a
	// negative limit carries no meaningful bound. In either case we return
	// nothing and leave the sweep history untouched rather than promoting the
	// value to a positive default, which would scan and record a sweep the
	// caller did not ask for.
	if limit <= 0 {
		return nil
	}
	expired := s.scanner.ScanByLimit(events, now, limit)
	// Copy so the recorded history is independent of the returned slice and
	// cannot be rewritten by a later sweep or by the caller.
	record := append([]string(nil), expired...)
	remaining := len(events) - len(expired)
	if remaining < 0 {
		remaining = 0
	}
	s.mu.Lock()
	s.sweeps = append(s.sweeps, Sweep{At: now, Expired: record, Remaining: remaining})
	if len(s.sweeps) > 32 {
		s.sweeps = s.sweeps[len(s.sweeps)-32:]
	}
	s.lastRun = now
	s.mu.Unlock()
	return expired
}

func (s *Sweeper) History() []Sweep {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]Sweep, len(s.sweeps))
	copy(result, s.sweeps)
	return result
}

func (s *Sweeper) LastRun() (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastRun.IsZero() {
		return time.Time{}, false
	}
	return s.lastRun, true
}

func (s *Sweeper) ExpiredIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := make(map[string]bool)
	for _, sweep := range s.sweeps {
		for _, id := range sweep.Expired {
			seen[id] = true
		}
	}
	result := make([]string, 0, len(seen))
	for id := range seen {
		result = append(result, id)
	}
	sort.Strings(result)
	return result
}
