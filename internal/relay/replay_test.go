package relay

import (
	"errors"
	"testing"
	"time"

	"example.com/relaydock/internal/core"
	"example.com/relaydock/internal/journal"
	"example.com/relaydock/internal/queue"
)

// TestReplayFullQueueKeepsOrderAndCursor verifies that a replay into a full
// scheduler returns a clear error, leaves the existing queue contents
// untouched (no eviction of the earliest-queued delivery), and does not
// advance the cursor past the entry it failed on.
func TestReplayFullQueueKeepsOrderAndCursor(t *testing.T) {
	now := time.Unix(1000, 0)
	log := &journal.Memory{}
	scheduler := queue.New(2)

	// Pre-fill the scheduler with two pending deliveries so the queue is full.
	if err := scheduler.Schedule("d-1", now.Add(-2*time.Second)); err != nil {
		t.Fatalf("pre-fill d-1: %v", err)
	}
	if err := scheduler.Schedule("d-2", now.Add(-1*time.Second)); err != nil {
		t.Fatalf("pre-fill d-2: %v", err)
	}

	// Journal holds two more pending transitions to replay on top of the full
	// queue; the first one must fail without disturbing anything.
	log.Append(journal.Entry{Delivery: "d-3", Transition: core.DeliveryPending, At: now})
	log.Append(journal.Entry{Delivery: "d-4", Transition: core.DeliveryPending, At: now.Add(time.Second)})

	replay := NewReplay(log, scheduler)
	processed, err := replay.Run(10)

	if !errors.Is(err, core.ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull, got processed=%d err=%v", processed, err)
	}
	// Only the pre-fill entries require no scheduling, so nothing from the
	// replay should have been processed as a pending entry before the failure.
	if processed != 0 {
		t.Fatalf("expected 0 processed, got %d", processed)
	}

	// The cursor must not have advanced past the failing entry.
	if c := replay.Cursor(); c != 0 {
		t.Fatalf("cursor advanced to %d on full queue, expected 0", c)
	}

	// The queue must be unchanged: still capacity entries, still the original
	// two ids, earliest first.
	if got := scheduler.Len(); got != 2 {
		t.Fatalf("scheduler len = %d, want 2 (no eviction, no push)", got)
	}
	first, ok := scheduler.PopReady(now.Add(time.Hour))
	if !ok || first != "d-1" {
		t.Fatalf("expected d-1 (FIFO) first, got %q ok=%v", first, ok)
	}
	second, ok := scheduler.PopReady(now.Add(time.Hour))
	if !ok || second != "d-2" {
		t.Fatalf("expected d-2 (FIFO) second, got %q ok=%v", second, ok)
	}
}

// TestReplayResumesAfterSlotFreesUp verifies that after a full-queue failure,
// draining a slot lets the next replay pick up exactly where it stopped — the
// evicted-and-lost behaviour is gone.
func TestReplayResumesAfterSlotFreesUp(t *testing.T) {
	now := time.Unix(1000, 0)
	log := &journal.Memory{}
	scheduler := queue.New(1)

	// One slot, already occupied.
	if err := scheduler.Schedule("d-1", now.Add(-time.Second)); err != nil {
		t.Fatalf("pre-fill d-1: %v", err)
	}

	// A pending transition to replay that will not fit.
	log.Append(journal.Entry{Delivery: "d-2", Transition: core.DeliveryPending, At: now})

	replay := NewReplay(log, scheduler)
	if _, err := replay.Run(10); !errors.Is(err, core.ErrQueueFull) {
		t.Fatalf("first run: expected ErrQueueFull, got %v", err)
	}

	// Free the slot and replay again — d-2 must now succeed and the cursor
	// must advance past it.
	if id, ok := scheduler.PopReady(now.Add(time.Hour)); !ok || id != "d-1" {
		t.Fatalf("expected to drain d-1, got %q ok=%v", id, ok)
	}
	processed, err := replay.Run(10)
	if err != nil {
		t.Fatalf("second run: unexpected err %v", err)
	}
	if processed != 1 {
		t.Fatalf("second run: expected 1 processed, got %d", processed)
	}
	if c := replay.Cursor(); c != 1 {
		t.Fatalf("cursor = %d, want 1 after replaying the entry", c)
	}
	// A third run should be a no-op now that the cursor is caught up.
	if processed, err := replay.Run(10); err != nil || processed != 0 {
		t.Fatalf("third run: expected 0/nil, got %d/%v", processed, err)
	}
}

// TestReplaySkipsNonPending verifies that non-pending transitions advance the
// cursor even when the queue is full, since they consume no queue slot.
func TestReplaySkipsNonPendingWhenFull(t *testing.T) {
	now := time.Unix(1000, 0)
	log := &journal.Memory{}
	scheduler := queue.New(1)

	if err := scheduler.Schedule("d-1", now.Add(-time.Second)); err != nil {
		t.Fatalf("pre-fill d-1: %v", err)
	}

	// A delivered transition queued behind the full scheduler: replay must
	// advance past it (no slot needed) and then hit the pending one.
	log.Append(journal.Entry{Delivery: "d-1", Transition: core.DeliveryDelivered, At: now})
	log.Append(journal.Entry{Delivery: "d-2", Transition: core.DeliveryPending, At: now.Add(time.Second)})

	replay := NewReplay(log, scheduler)
	processed, err := replay.Run(10)
	if !errors.Is(err, core.ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull on the pending entry, got processed=%d err=%v", processed, err)
	}
	// The delivered transition was advanced over (1 processed), the pending
	// one was not.
	if processed != 1 {
		t.Fatalf("expected 1 processed (delivered), got %d", processed)
	}
	if c := replay.Cursor(); c != 1 {
		t.Fatalf("cursor = %d, want 1 (advanced over delivered, stopped at pending)", c)
	}
}
