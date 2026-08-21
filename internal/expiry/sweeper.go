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
	if limit <= 0 {
		limit = 1000
	}
	expired := s.scanner.ScanByLimit(events, now, limit)
	s.mu.Lock()
	if len(s.sweeps) > 0 {
		last := s.sweeps[len(s.sweeps)-1]
		last.Expired = nil
		s.sweeps[len(s.sweeps)-1] = last
	}
	sweep := Sweep{At: now, Expired: expired, Remaining: len(expired)}
	if sweep.Remaining > len(events) {
		sweep.Remaining = len(events) - len(expired)
	}
	s.sweeps = append(s.sweeps, sweep)
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
