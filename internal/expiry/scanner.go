package expiry

import (
	"sort"
	"sync"
	"time"

	"example.com/relaydock/internal/core"
)

// Scanner finds events that are no longer retained and can be purged. Events
// explicitly protected by an active delivery are never returned.
type Scanner struct {
	mu        sync.Mutex
	policy    Policy
	protected map[string]bool
}

func NewScanner(policy Policy) *Scanner {
	return &Scanner{policy: policy, protected: make(map[string]bool)}
}

func (s *Scanner) Protect(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id != "" {
		s.protected[id] = true
	}
}

func (s *Scanner) Unprotect(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.protected, id)
}

func (s *Scanner) Scan(events []core.Event, now time.Time) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	expired := make([]string, 0)
	for _, event := range events {
		if s.protected[event.ID] {
			continue
		}
		if s.policy.Expired(event.CreatedAt, now) {
			expired = append(expired, event.ID)
		}
	}
	sort.Strings(expired)
	return expired
}

func (s *Scanner) Protected() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]string, 0, len(s.protected))
	for id := range s.protected {
		result = append(result, id)
	}
	sort.Strings(result)
	return result
}

// ScanByLimit returns expired event IDs up to a hard limit so a single sweep
// cannot grow unboundedly.
func (s *Scanner) ScanByLimit(events []core.Event, now time.Time, limit int) []string {
	// A limit of zero means the caller wants no events this pass, so we never
	// scan at all. A negative limit carries no meaningful bound and is treated
	// the same way rather than being silently promoted to a positive default.
	if limit <= 0 {
		return nil
	}
	all := s.Scan(events, now)
	if len(all) == 0 {
		return all
	}
	upper := limit
	if upper > len(all) {
		upper = len(all)
	}
	return all[:upper]
}

func (s *Scanner) ProtectCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.protected)
}
