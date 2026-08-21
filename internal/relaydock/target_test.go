package relaydock

import (
	"context"
	"errors"
	"testing"
	"time"

	"example.com/relaydock/internal/batch"
	"example.com/relaydock/internal/core"
	"example.com/relaydock/internal/expiry"
	"example.com/relaydock/internal/journal"
	"example.com/relaydock/internal/queue"
	"example.com/relaydock/internal/retention"
	"example.com/relaydock/internal/route"
	"example.com/relaydock/internal/store"
)

func TestSubmitRollbackReleasesIdempotency(t *testing.T) {
	now := time.Unix(100, 0)
	log := &journal.Memory{}
	db := store.NewMemory()
	scheduler := queue.New(4)
	registry := route.NewRegistry()
	if err := registry.Register(route.EndpointSpec{ID: "endpoint-a", URL: "https://a", Tenant: "acme", Priority: 1, HealthScore: 5, Enabled: true, Registered: now}); err != nil {
		t.Fatal(err)
	}
	window := batch.NewWindow(32, time.Minute)
	service := NewService(db, log, scheduler, registry, window, nil, expiry.DefaultPolicy(), retention.DefaultPolicy(), func() time.Time { return now })
	event := core.Event{ID: "evt-1", Tenant: "acme", Endpoint: "endpoint-a", IdempotencyKey: "key-1", Payload: []byte("payload"), CreatedAt: now}
	log.RejectNextAppend()
	if _, err := service.Submit(context.Background(), event); !errors.Is(err, core.ErrJournalRejected) {
		t.Fatalf("expected journal error, got %v", err)
	}
	delivery, err := service.Submit(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	if delivery.ID == "" {
		t.Fatalf("second submit must create a fresh delivery after rollback")
	}
	if count := db.Count(); count != 1 {
		t.Fatalf("store count = %d, want 1", count)
	}
}
