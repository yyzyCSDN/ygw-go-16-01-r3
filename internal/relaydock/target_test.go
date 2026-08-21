package relaydock

import (
	"context"
	"errors"
	"testing"
	"time"

	"example.com/relaydock/internal/core"
	"example.com/relaydock/internal/journal"
	"example.com/relaydock/internal/queue"
	"example.com/relaydock/internal/relay"
	"example.com/relaydock/internal/store"
)

func TestStepBatchStopsOnFailure(t *testing.T) {
	now := time.Unix(100, 0)
	log := &journal.Memory{}
	db := store.NewMemory()
	scheduler := queue.New(4)
	event := core.Event{ID: "evt-9", Tenant: "acme", Endpoint: "https://sink", IdempotencyKey: "key-9", Payload: []byte("data"), CreatedAt: now}
	db.Reserve(event, "d1", now)
	db.Reserve(event, "d2", now)
	worker := relay.Worker{
		ID: "w", Store: db, Journal: log, Queue: scheduler,
		Breakers: relay.NewBreakers(2, time.Minute),
		Sender: relay.SenderFunc(func(context.Context, core.Event) error { return errors.New("boom") }),
		Now: func() time.Time { return now }, LeaseTTL: time.Minute, MaxAttempts: 3,
	}
	completed, err := worker.StepBatch(context.Background(), []string{"d1", "d2"})
	if err == nil {
		t.Fatalf("StepBatch must stop and surface the failure, completed=%d", completed)
	}
	if completed != 0 {
		t.Fatalf("completed = %d, want 0", completed)
	}
}
