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
