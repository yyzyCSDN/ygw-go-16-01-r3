package expiry

import (
	"testing"
	"time"

	"example.com/relaydock/internal/core"
)

func newTestScanner() *Scanner {
	return NewScanner(Policy{TTL: time.Hour})
}

func expiredEvents(now time.Time, ids ...string) []core.Event {
	events := make([]core.Event, 0, len(ids))
	for _, id := range ids {
		events = append(events, core.Event{ID: id, CreatedAt: now.Add(-2 * time.Hour)})
	}
	return events
}

// ScanByLimit with a zero limit must not scan at all: it returns nothing and
// never falls back to scanning one event.
func TestScannerScanByLimitZeroDoesNotScan(t *testing.T) {
	sc := newTestScanner()
	now := time.Now()
	events := expiredEvents(now, "evt-a")
	got := sc.ScanByLimit(events, now, 0)
	if len(got) != 0 {
		t.Fatalf("limit=0 should not scan, got %v", got)
	}
}

// ScanByLimit must truncate results to the requested limit instead of returning
// every expired event at once.
func TestScannerScanByLimitTruncates(t *testing.T) {
	sc := newTestScanner()
	now := time.Now()
	events := expiredEvents(now, "evt-a", "evt-b", "evt-c", "evt-d")
	got := sc.ScanByLimit(events, now, 2)
	if len(got) != 2 {
		t.Fatalf("limit=2 should truncate to 2, got %d: %v", len(got), got)
	}
}

// Sweeper.Run with a zero limit must not scan and must not record a sweep.
func TestSweeperRunZeroLimitDoesNotScan(t *testing.T) {
	s := NewSweeper(newTestScanner(), time.Minute)
	now := time.Now()
	events := expiredEvents(now, "evt-a")
	got := s.Run(events, now, 0)
	if len(got) != 0 {
		t.Fatalf("limit=0 should not scan, got %v", got)
	}
	if h := s.History(); len(h) != 0 {
		t.Fatalf("limit=0 must not record a sweep, got history %v", h)
	}
	if _, ok := s.LastRun(); ok {
		t.Fatal("limit=0 must not advance lastRun")
	}
}

// Sweeper.Run must truncate results to the limit instead of returning all
// expired events at once.
func TestSweeperRunTruncates(t *testing.T) {
	s := NewSweeper(newTestScanner(), time.Minute)
	now := time.Now()
	events := expiredEvents(now, "evt-a", "evt-b", "evt-c", "evt-d")
	got := s.Run(events, now, 2)
	if len(got) != 2 {
		t.Fatalf("limit=2 should truncate to 2, got %d: %v", len(got), got)
	}
}

// Each completed sweep is appended to history and is not rewritten by a later
// sweep — history grows by one per run and prior entries keep their Expired IDs.
func TestSweeperRunHistoryIsAppendOnly(t *testing.T) {
	s := NewSweeper(newTestScanner(), time.Minute)
	now := time.Now()

	first := s.Run(expiredEvents(now, "evt-a"), now, 100)
	second := s.Run(expiredEvents(now, "evt-b"), now.Add(time.Second), 100)

	h := s.History()
	if len(h) != 2 {
		t.Fatalf("history should retain 2 sweeps, got %d", len(h))
	}
	if len(h[0].Expired) != 1 || h[0].Expired[0] != "evt-a" {
		t.Fatalf("first sweep rewritten: %v", h[0].Expired)
	}
	if len(h[1].Expired) != 1 || h[1].Expired[0] != "evt-b" {
		t.Fatalf("second sweep wrong: %v", h[1].Expired)
	}
	// Returned slice must be independent of the recorded history: mutating the
	// return must not corrupt later history snapshots.
	second[0] = "mutated"
	if h2 := s.History(); h2[1].Expired[0] != "evt-b" {
		t.Fatalf("history corrupted by caller mutation: %v", h2[1].Expired)
	}
	_ = first
}

// Remaining reports the events left after the sweep, not the number expired.
func TestSweeperRunRemainingIsEventsLeft(t *testing.T) {
	s := NewSweeper(newTestScanner(), time.Minute)
	now := time.Now()
	// 4 events total, all expired; with limit 2 the sweeper expires 2 and 2 remain.
	events := expiredEvents(now, "evt-a", "evt-b", "evt-c", "evt-d")
	s.Run(events, now, 2)
	h := s.History()
	if h[0].Remaining != 2 {
		t.Fatalf("Remaining should be events left (2), got %d", h[0].Remaining)
	}
}
