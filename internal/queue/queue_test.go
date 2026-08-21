package queue

import (
	"errors"
	"testing"
	"time"

	"example.com/relaydock/internal/core"
)

// TestSchedulerFullRejectsAndPreservesOrder verifies that once the scheduler is
// full, Schedule returns ErrQueueFull, the new id is NOT enqueued, and the
// earliest-queued id is NOT evicted — FIFO order is preserved exactly.
func TestSchedulerFullRejectsAndPreservesOrder(t *testing.T) {
	now := time.Unix(1000, 0)
	s := New(2)

	if err := s.Schedule("d-1", now.Add(-2*time.Second)); err != nil {
		t.Fatalf("schedule d-1: %v", err)
	}
	if err := s.Schedule("d-2", now.Add(-1*time.Second)); err != nil {
		t.Fatalf("schedule d-2: %v", err)
	}

	// Queue is full: a third delivery must be rejected, not swapped in.
	err := s.Schedule("d-3", now)
	if !errors.Is(err, core.ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}

	if got := s.Len(); got != 2 {
		t.Fatalf("len = %d, want 2 (no eviction, no push)", got)
	}

	// Drain in FIFO order: d-1 then d-2 — d-1 must still be there (no eviction).
	first, ok := s.PopReady(now.Add(time.Hour))
	if !ok || first != "d-1" {
		t.Fatalf("expected d-1 first, got %q ok=%v", first, ok)
	}
	second, ok := s.PopReady(now.Add(time.Hour))
	if !ok || second != "d-2" {
		t.Fatalf("expected d-2 second, got %q ok=%v", second, ok)
	}
}

// TestSchedulerUpdateExistingDoesNotGrow verifies that rescheduling an id
// already in the queue updates its ready time in place rather than counting
// as a new entry (and is therefore not subject to capacity rejection).
func TestSchedulerUpdateExistingDoesNotGrow(t *testing.T) {
	now := time.Unix(1000, 0)
	s := New(1)

	if err := s.Schedule("d-1", now.Add(time.Second)); err != nil {
		t.Fatalf("schedule d-1: %v", err)
	}
	// Update the existing entry — capacity is 1 but this is an update, not a
	// new insert, so it must succeed.
	if err := s.Schedule("d-1", now.Add(-time.Second)); err != nil {
		t.Fatalf("update d-1: %v", err)
	}
	if got := s.Len(); got != 1 {
		t.Fatalf("len = %d, want 1", got)
	}
	if id, ok := s.Peek(now); !ok || id != "d-1" {
		t.Fatalf("expected updated d-1 to be ready, got %q ok=%v", id, ok)
	}
}
