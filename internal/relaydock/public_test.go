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

func newFixture(t *testing.T) (*Service, time.Time) {
	t.Helper()
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	db := store.NewMemory()
	log := &journal.Memory{}
	scheduler := queue.New(64)
	registry := route.NewRegistry()
	if err := registry.Register(route.EndpointSpec{
		ID: "endpoint-a", URL: "https://sink-a", Tenant: "acme", Priority: 1, HealthScore: 5, Enabled: true, Registered: now,
	}); err != nil {
		t.Fatal(err)
	}
	window := batch.NewWindow(32, time.Minute)
	svc := NewService(db, log, scheduler, registry, window, nil, expiry.DefaultPolicy(), retention.DefaultPolicy(), func() time.Time { return now })
	return svc, now
}

func TestRelayDockSubmitDispatchConfirm(t *testing.T) {
	svc, now := newFixture(t)
	event := core.Event{ID: "evt-1", Tenant: "acme", Endpoint: "endpoint-a", IdempotencyKey: "key-1", Payload: []byte("payload"), CreatedAt: now}
	delivery, err := svc.Submit(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	if delivery.State != core.DeliveryPending {
		t.Fatalf("state = %s", delivery.State)
	}
	window := svc.WindowSnapshot()
	if len(window) != 1 {
		t.Fatalf("window items = %d", len(window))
	}
	delivered, err := svc.Dispatch(context.Background(), func(context.Context, core.Event) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if delivered != 1 {
		t.Fatalf("delivered = %d", delivered)
	}
	if err := svc.ConfirmDelivery(delivery.ID); err != nil {
		t.Fatal(err)
	}
	if open := svc.OpenDeliveries(); len(open) != 0 {
		t.Fatalf("open deliveries = %v", open)
	}
	if confirmations := svc.ReceiptsSnapshot(); len(confirmations) != 1 {
		t.Fatalf("confirmations = %d", len(confirmations))
	}
}

func TestRelayDockFailureTracksDead(t *testing.T) {
	svc, now := newFixture(t)
	event := core.Event{ID: "evt-2", Tenant: "acme", Endpoint: "endpoint-a", IdempotencyKey: "key-2", Payload: []byte("data"), CreatedAt: now}
	if _, err := svc.Submit(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	sendErr := errors.New("sink unavailable")
	if _, err := svc.Dispatch(context.Background(), func(context.Context, core.Event) error { return sendErr }); err != nil {
		t.Fatal(err)
	}
	if dead := svc.Retention.DeadCount(); dead != 1 {
		t.Fatalf("dead count = %d", dead)
	}
}

func TestRelayDockExpiryRespectsProtection(t *testing.T) {
	svc, now := newFixture(t)
	old := now.Add(-48 * time.Hour)
	event := core.Event{ID: "evt-3", Tenant: "acme", Endpoint: "endpoint-a", IdempotencyKey: "key-3", Payload: []byte("stale"), CreatedAt: old}
	svc.Store.Reserve(event, "delivery-expired", now)
	svc.Expiry.Protect("evt-3")
	if expired := svc.ExpireEvents(); len(expired) != 0 {
		t.Fatalf("protected event expired: %v", expired)
	}
	svc.Expiry.Unprotect("evt-3")
	if expired := svc.ExpireEvents(); len(expired) != 1 || expired[0] != "evt-3" {
		t.Fatalf("expired = %v", expired)
	}
}

func TestRelayDockSupervisorRuns(t *testing.T) {
	svc, now := newFixture(t)
	event := core.Event{ID: "evt-s", Tenant: "acme", Endpoint: "endpoint-a", IdempotencyKey: "key-s", Payload: []byte("data"), CreatedAt: now}
	delivery, err := svc.Submit(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Dispatch(context.Background(), func(context.Context, core.Event) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := svc.ConfirmDelivery(delivery.ID); err != nil {
		t.Fatal(err)
	}
	_ = svc.ReceiptSignature(delivery.ID, "endpoint-a", 1)
	result := svc.RunSupervision(now.Add(time.Minute))
	if result.HealthAdjustments == nil {
		t.Fatal("supervisor must evaluate health")
	}
	if svc.Supervisor.Report(now.Add(time.Minute)) == "" {
		t.Fatal("supervisor report must be non-empty")
	}
}

func TestRelayDockVerifyReceipt(t *testing.T) {
	svc, now := newFixture(t)
	event := core.Event{ID: "evt-v", Tenant: "acme", Endpoint: "endpoint-a", IdempotencyKey: "key-v", Payload: []byte("data"), CreatedAt: now}
	delivery, err := svc.Submit(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Dispatch(context.Background(), func(context.Context, core.Event) error { return nil }); err != nil {
		t.Fatal(err)
	}
	signature := svc.ReceiptSignature(delivery.ID, "endpoint-a", 1)
	if err := svc.VerifyReceipt(delivery.ID, "endpoint-a", 1, signature); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}
	if err := svc.VerifyReceipt(delivery.ID, "endpoint-a", 1, "forged"); err == nil {
		t.Fatal("forged signature must be rejected")
	}
}

// TestRelayDockJournalFailureReleasesIdempotencyKey guards the failed-admission
// rollback path: when the journal rejects the pending entry, the idempotency
// reservation must be fully released so a retry with the same key creates a
// brand-new delivery that actually enters the queue and window.
func TestRelayDockJournalFailureReleasesIdempotencyKey(t *testing.T) {
	svc, now := newFixture(t)
	log := svc.Journal
	event := core.Event{ID: "evt-retry", Tenant: "acme", Endpoint: "endpoint-a", IdempotencyKey: "key-retry", Payload: []byte("data"), CreatedAt: now}

	log.RejectNextAppend()
	if _, err := svc.Submit(context.Background(), event); !errors.Is(err, core.ErrJournalRejected) {
		t.Fatalf("first submit should fail with journal error, got %v", err)
	}
	if active := svc.ActiveCount(); active != 0 {
		t.Fatalf("no delivery should remain after journal failure, active=%d", active)
	}

	delivery, err := svc.Submit(context.Background(), event)
	if err != nil {
		t.Fatalf("retry submit should succeed, got %v", err)
	}
	if delivery.ID == "" {
		t.Fatal("retry must produce a real delivery, got empty id")
	}
	if ready := svc.QueueReadyCount(now); ready != 1 {
		t.Fatalf("retry must enqueue the delivery, ready=%d", ready)
	}
	if items := svc.WindowSnapshot(); len(items) != 1 || items[0].ID != delivery.ID {
		t.Fatalf("retry must place the delivery in the window, got %+v", items)
	}
}

func TestRelayDockPriorityWindow(t *testing.T) {
	svc, now := newFixture(t)
	low := core.Event{ID: "evt-low", Tenant: "acme", Endpoint: "endpoint-a", IdempotencyKey: "key-low", Payload: []byte("a"), Priority: 5, CreatedAt: now}
	high := core.Event{ID: "evt-high", Tenant: "acme", Endpoint: "endpoint-a", IdempotencyKey: "key-high", Payload: []byte("b"), Priority: 1, CreatedAt: now}
	lowDelivery, err := svc.Submit(context.Background(), low)
	if err != nil {
		t.Fatal(err)
	}
	highDelivery, err := svc.Submit(context.Background(), high)
	if err != nil {
		t.Fatal(err)
	}
	order := svc.Priority.PriorityOrder()
	if len(order) != 2 || order[0].ID != highDelivery.ID || order[1].ID != lowDelivery.ID {
		t.Fatalf("priority order wrong: %+v", order)
	}
}
