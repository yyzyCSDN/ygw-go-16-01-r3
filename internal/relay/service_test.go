package relay

import (
	"context"
	"errors"
	"testing"
	"time"

	"example.com/relaydock/internal/core"
	"example.com/relaydock/internal/journal"
	"example.com/relaydock/internal/queue"
	"example.com/relaydock/internal/store"
)

func TestSubmitRollbackAndDeliver(t *testing.T) {
	now := time.Unix(100, 0)
	log := &journal.Memory{}
	db := store.NewMemory()
	scheduler := queue.New(4)
	service := Service{Store: db, Journal: log, Queue: scheduler, Now: func() time.Time { return now }}
	event := core.Event{ID: "evt-1", Tenant: "acme", Endpoint: "https://sink", IdempotencyKey: "key-1", Payload: []byte("payload"), CreatedAt: now}
	log.RejectNextAppend()
	if _, _, err := service.Submit(event); !errors.Is(err, core.ErrJournalRejected) {
		t.Fatalf("expected journal error, got %v", err)
	}
	delivery, created, err := service.Submit(event)
	if err != nil || !created {
		t.Fatalf("second submit should recover reservation: created=%v err=%v", created, err)
	}
	worker := Worker{ID: "worker-a", Store: db, Journal: log, Queue: scheduler, Breakers: NewBreakers(2, time.Minute), Sender: SenderFunc(func(context.Context, core.Event) error { return nil }), Now: func() time.Time { return now }, LeaseTTL: time.Minute, MaxAttempts: 3}
	if delivered, err := worker.Step(context.Background()); err != nil || !delivered {
		t.Fatalf("delivery failed: delivered=%v err=%v", delivered, err)
	}
	stored, _ := db.Get(delivery.ID)
	if stored.State != core.DeliveryDelivered {
		t.Fatalf("unexpected state %s", stored.State)
	}
}

// TestLeaseFencingInvalidatesStaleEpoch proves the core lease-fencing
// invariant: after a lease expires and is reacquired, the epoch from the
// expired lease must no longer be accepted by Complete/Retry/Dead. Were the
// epoch reused on reacquire (the bug), the stale credentials could mark the
// same delivery complete a second time.
func TestLeaseFencingInvalidatesStaleEpoch(t *testing.T) {
	now := time.Unix(100, 0)
	db := store.NewMemory()
	event := core.Event{ID: "evt-f", Tenant: "acme", Endpoint: "https://sink", IdempotencyKey: "key-f", Payload: []byte("payload"), CreatedAt: now}
	db.Reserve(event, "delivery-f", now)

	// First acquisition: worker-a holds epoch 1, then lets the lease lapse.
	acquired, err := db.Acquire("delivery-f", "worker-a", now, time.Minute)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	staleEpoch := acquired.LeaseEpoch
	if staleEpoch != 1 {
		t.Fatalf("initial epoch = %d, want 1", staleEpoch)
	}

	// Lease lapses; the same worker reacquires after expiry. The new epoch
	// must be strictly greater than the stale one.
	reacquired, err := db.Acquire("delivery-f", "worker-a", now.Add(2*time.Minute), time.Minute)
	if err != nil {
		t.Fatalf("reacquire after expiry: %v", err)
	}
	if reacquired.LeaseEpoch <= staleEpoch {
		t.Fatalf("epoch not bumped on reacquire: stale=%d new=%d", staleEpoch, reacquired.LeaseEpoch)
	}

	// The stale credentials from the first lease must be rejected: a
	// completion attempt with the old epoch must fail, while the new epoch
	// succeeds. This is what prevents a lapsed holder from double-completing.
	if _, err := db.Complete("delivery-f", "worker-a", staleEpoch); !errors.Is(err, core.ErrLeaseConflict) {
		t.Fatalf("stale epoch Complete: err=%v, want %v", err, core.ErrLeaseConflict)
	}
	if _, err := db.Retry("delivery-f", "worker-a", staleEpoch, now.Add(time.Second), errors.New("stale")); !errors.Is(err, core.ErrLeaseConflict) {
		t.Fatalf("stale epoch Retry: err=%v, want %v", err, core.ErrLeaseConflict)
	}

	// The fresh epoch completes exactly once. A second complete with the same
	// (now consumed) epoch is rejected because the delivery is no longer
	// leased — the delivery cannot be completed twice.
	if _, err := db.Complete("delivery-f", "worker-a", reacquired.LeaseEpoch); err != nil {
		t.Fatalf("fresh epoch Complete: err=%v", err)
	}
	if _, err := db.Complete("delivery-f", "worker-a", reacquired.LeaseEpoch); !errors.Is(err, core.ErrLeaseConflict) {
		t.Fatalf("second Complete: err=%v, want %v", err, core.ErrLeaseConflict)
	}
}
