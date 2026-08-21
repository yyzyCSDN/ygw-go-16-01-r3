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

func TestBreakerResetsFailuresAfterProbeRecovery(t *testing.T) {
	const endpoint = "https://sink"
	breakers := NewBreakers(2, time.Minute)
	now := time.Unix(1000, 0)

	// Accumulate up to the threshold so the circuit opens.
	for i := 0; i < 2; i++ {
		release, err := breakers.Allow(endpoint, now)
		if err != nil {
			t.Fatalf("allow %d: %v", i, err)
		}
		release(false)
	}
	if !breakers.IsOpen(endpoint, now) {
		t.Fatalf("circuit should be open after threshold failures")
	}

	// Cooldown elapses; a half-open probe is dispatched.
	after := now.Add(2 * time.Minute)
	release, err := breakers.Allow(endpoint, after)
	if err != nil {
		t.Fatalf("probe allow: %v", err)
	}
	// Probe succeeds: this must reset the failure count and clear the circuit.
	release(true)

	if breakers.IsOpen(endpoint, after) {
		t.Fatalf("circuit should be closed after successful probe")
	}
	if got := breakers.FailuresFor(endpoint); got != 0 {
		t.Fatalf("failures after successful probe = %d, want 0", got)
	}

	// A single subsequent failure must not reopen the circuit; the endpoint
	// needs to re-accumulate to the threshold.
	release, err = breakers.Allow(endpoint, after)
	if err != nil {
		t.Fatalf("post-probe allow: %v", err)
	}
	release(false)
	if breakers.IsOpen(endpoint, after) {
		t.Fatalf("circuit must not reopen on a single failure after recovery; failures should re-accumulate from zero")
	}
	if got := breakers.FailuresFor(endpoint); got != 1 {
		t.Fatalf("failures after one post-recovery failure = %d, want 1", got)
	}
}

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
