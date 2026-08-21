package main

import (
	"context"
	"fmt"
	"time"

	"example.com/relaydock/internal/batch"
	"example.com/relaydock/internal/core"
	"example.com/relaydock/internal/expiry"
	"example.com/relaydock/internal/journal"
	"example.com/relaydock/internal/queue"
	"example.com/relaydock/internal/receipt"
	"example.com/relaydock/internal/relay"
	"example.com/relaydock/internal/relaydock"
	"example.com/relaydock/internal/retention"
	"example.com/relaydock/internal/route"
	"example.com/relaydock/internal/store"
)

func main() {
	now := time.Now
	db := store.NewMemory()
	log := &journal.Memory{}
	scheduler := queue.New(64)
	registry := route.NewRegistry()
	endpoint := route.EndpointSpec{ID: "demo-endpoint", URL: "https://example.invalid/hook", Tenant: "local", Priority: 1, HealthScore: 5, Enabled: true, Registered: now()}
	_ = registry.Register(endpoint)
	window := batch.NewWindow(16, time.Minute)
	service := relaydock.NewService(db, log, scheduler, registry, window, nil, expiry.DefaultPolicy(), retention.DefaultPolicy(), now)
	event := core.Event{ID: "demo", Tenant: "local", Endpoint: "demo-endpoint", IdempotencyKey: "demo-1", Payload: []byte("hello"), Priority: 1, CreatedAt: now()}
	delivery, err := service.Submit(context.Background(), event)
	if err != nil {
		fmt.Printf("submit err=%v\n", err)
		return
	}

	delivered, _ := service.Dispatch(context.Background(), relay.SenderFunc(func(context.Context, core.Event) error { return nil }))
	// The batch window has not elapsed yet, so the demo confirms the delivery
	// out-of-band to exercise the receipt verification path.
	_ = service.Receipts.Record(receipt.Receipt{DeliveryID: delivery.ID, Endpoint: "demo-endpoint", Attempt: 1, Status: "delivered", At: now()})
	service.ReceiptLedger.Open(delivery.ID)
	signature := service.ReceiptSignature(delivery.ID, "demo-endpoint", 1)
	verified := service.VerifyReceipt(delivery.ID, "demo-endpoint", 1, signature) == nil

	breakers := relay.NewBreakers(2, time.Minute)
	worker := relay.Worker{ID: "demo-worker", Store: db, Journal: log, Queue: scheduler, Breakers: breakers, Sender: relay.SenderFunc(func(context.Context, core.Event) error { return nil }), Now: now, LeaseTTL: time.Minute, MaxAttempts: 3}
	backoff := worker.BackoffFor(2)
	openBefore := breakers.IsOpen("demo-endpoint", now())
	failuresBefore := breakers.FailuresFor("demo-endpoint")
	_, _ = worker.StepBatch(context.Background(), []string{delivery.ID})
	failuresAfter := breakers.FailuresFor("demo-endpoint")
	breakers.Reset("demo-endpoint")

	replay := relay.NewReplay(log, scheduler)
	_, _ = replay.RunAll()
	remaining := replay.Remaining()
	peekedID, peeked := scheduler.Peek(now())

	_ = registry.Disable("demo-endpoint")
	enabledAfterDisable := registry.EnabledCount()
	_ = registry.Enable("demo-endpoint")
	enabledAfterEnable := registry.EnabledCount()
	fallback, fallbackErr := service.Planner.Fallback(event, "demo-endpoint")
	usable := service.Planner.UsableCount("local")
	adjustedHealth, _ := service.HealthAdjust("demo-endpoint", 1)
	windowInterval := service.Window.Interval()
	_ = registry.Deregister("demo-endpoint")
	deregistered := registry.Count()
	_ = registry.Register(endpoint)
	_ = fallback
	_ = fallbackErr

	result := service.RunSupervision(now().Add(time.Minute))
	healthAdjustments := len(result.HealthAdjustments)
	_ = service.Supervisor.Sweeper.ExpiredIDs()
	_, sweeperLast := service.Supervisor.Sweeper.LastRun()
	_, reaperLast := service.Supervisor.Reaper.LastRun()
	protected := service.Expiry.ProtectCount()

	fmt.Printf("delivery=%s delivered=%d verified=%v backoff=%s open_before=%v failures_before=%d failures_after=%d remaining=%d peek=%v/%s enabled_disable=%d enable=%d usable=%d health=%d health_adjustments=%d interval=%s deregistered=%d sweeper_last=%v reaper_last=%v protected=%d window=%d journal=%d store=%d ready=%d\n",
		delivery.ID, delivered, verified, backoff, openBefore, failuresBefore, failuresAfter, remaining, peeked, peekedID,
		enabledAfterDisable, enabledAfterEnable, usable, adjustedHealth, healthAdjustments, windowInterval, deregistered, sweeperLast, reaperLast, protected,
		service.Window.Len(), service.JournalCount(), service.StoreCount(), service.QueueReadyCount(now()))
	fmt.Printf("supervision=%s\n", service.Supervisor.Report(now()))
	fmt.Printf("summary=%s capacity_window=%d queue=%d batches=%d confirmations=%d open=%d dead=%d\n",
		core.Summarize(service.DeliveriesByState(core.DeliveryPending)).String(),
		service.WindowCapacity(), service.QueueCapacity(), service.FlushedBatches(),
		len(service.ReceiptsSnapshot()), len(service.OpenDeliveries()), service.Retention.DeadCount())
}
