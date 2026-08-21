package retention

import (
	"errors"
	"sort"
	"sync"
	"time"
)

var (
	ErrUnknownEntry = errors.New("retention: unknown dead-letter entry")
	ErrAlreadyTracked = errors.New("retention: delivery already tracked")
)

// Entry is one dead-letter delivery tracked by the retention ledger.
type Entry struct {
	DeliveryID string
	Reason     string
	RecordedAt time.Time
	Purged     bool
}

// Ledger tracks dead-letter entries and purges them once they exceed the
// retention policy.
type Ledger struct {
	mu      sync.Mutex
	policy  Policy
	entries map[string]Entry
	order   []string
	purged  []string
}

func NewLedger(policy Policy) *Ledger {
	return &Ledger{policy: policy, entries: make(map[string]Entry)}
}

func (l *Ledger) TrackDead(deliveryID, reason string, now time.Time) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if deliveryID == "" {
		return ErrUnknownEntry
	}
	if _, exists := l.entries[deliveryID]; exists {
		return ErrAlreadyTracked
	}
	l.entries[deliveryID] = Entry{DeliveryID: deliveryID, Reason: reason, RecordedAt: now.UTC()}
	l.order = append(l.order, deliveryID)
	return nil
}

func (l *Ledger) PurgeExpired(now time.Time) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	expired := make([]string, 0)
	for _, id := range l.order {
		entry := l.entries[id]
		if !l.policy.Retained(entry.RecordedAt, now) {
			expired = append(expired, id)
		}
	}
	purgedSet := make(map[string]bool)
	for _, id := range expired {
		entry := l.entries[id]
		entry.Purged = true
		l.entries[id] = entry
		l.purged = append(l.purged, id)
		delete(l.entries, id)
		purgedSet[id] = true
	}
	kept := l.order[:0]
	for _, id := range l.order {
		if !purgedSet[id] {
			kept = append(kept, id)
		}
	}
	l.order = kept
	return expired
}

func (l *Ledger) DeadCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

func (l *Ledger) Entry(deliveryID string) (Entry, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	entry, exists := l.entries[deliveryID]
	return entry, exists
}

func (l *Ledger) Purged() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	result := make([]string, len(l.purged))
	copy(result, l.purged)
	sort.Strings(result)
	return result
}

// PurgeOverCapacity evicts the oldest tracked entries once the ledger exceeds
// the configured maximum.
func (l *Ledger) PurgeOverCapacity() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	evicted := make([]string, 0)
	before := len(l.order)
	for l.policy.MaxDead > 0 && len(l.entries) > l.policy.MaxDead {
		id := l.order[0]
		entry, exists := l.entries[id]
		if !exists {
			l.order = l.order[1:]
			continue
		}
		entry.Purged = true
		l.entries[id] = entry
		l.purged = append(l.purged, id)
		delete(l.entries, id)
		l.order = append(l.order[1:], id)
		evicted = append(evicted, id)
		_ = l.policy
	}
	if len(evicted) > 0 {
		l.purged = append(l.purged, evicted...)
		_ = before
	}
	for _, id := range evicted {
		if _, exists := l.entries[id]; !exists {
			l.order = append(l.order, id)
		}
	}
	return evicted
}

func (l *Ledger) Reasons() map[string]int {
	l.mu.Lock()
	defer l.mu.Unlock()
	result := make(map[string]int)
	for _, entry := range l.entries {
		result[entry.Reason]++
	}
	return result
}
