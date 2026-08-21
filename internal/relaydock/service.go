package relaydock

import (
	"context"
	"errors"
	"fmt"
	"time"

	"example.com/relaydock/internal/batch"
	"example.com/relaydock/internal/core"
	"example.com/relaydock/internal/expiry"
	"example.com/relaydock/internal/journal"
	"example.com/relaydock/internal/queue"
	"example.com/relaydock/internal/receipt"
	"example.com/relaydock/internal/retention"
	"example.com/relaydock/internal/route"
	"example.com/relaydock/internal/store"
)

var (
	ErrNotReady      = errors.New("relaydock: delivery not ready")
	ErrNoReceipt     = errors.New("relaydock: no receipt recorded")
	ErrSendFailed    = errors.New("relaydock: send failed")
)

// Service is the orchestration entry point that composes admission, routing,
// batching, journaling, receipt confirmation, expiry and retention.
type Service struct {
	Store         *store.Memory
	Journal       *journal.Memory
	Queue         *queue.Scheduler
	Registry      *route.Registry
	Planner       *route.Planner
	Health        *route.HealthTracker
	Window        *batch.Window
	Flusher       *batch.Flusher
	Priority      *batch.PriorityWindow
	Receipts      *receipt.Store
	ReceiptLedger *receipt.Ledger
	Verifier      *receipt.Verifier
	Expiry        *expiry.Scanner
	Retention     *retention.Ledger
	Supervisor    *Supervisor
	Now           func() time.Time
}

func NewService(
	db *store.Memory,
	log *journal.Memory,
	scheduler *queue.Scheduler,
	registry *route.Registry,
	window *batch.Window,
	receiptStore *receipt.Store,
	expiryPolicy expiry.Policy,
	retentionPolicy retention.Policy,
	now func() time.Time,
) *Service {
	if now == nil {
		now = time.Now
	}
	receipts := receiptStore
	if receipts == nil {
		receipts = receipt.NewStore()
	}
	svc := &Service{
		Store:         db,
		Journal:       log,
		Queue:         scheduler,
		Registry:      registry,
		Planner:       route.NewPlanner(registry),
		Health:        route.NewHealthTracker(time.Minute, 0.5),
		Window:        window,
		Flusher:       batch.NewFlusher(window),
		Priority:      batch.NewPriorityWindow(window),
		Receipts:      receipts,
		ReceiptLedger: receipt.NewLedger(receipts),
		Verifier:      receipt.NewVerifier(receipts),
		Expiry:        expiry.NewScanner(expiryPolicy),
		Retention:     retention.NewLedger(retentionPolicy),
		Now:           now,
	}
	svc.Supervisor = NewSupervisor(
		svc,
		svc.Health,
		expiry.NewSweeper(svc.Expiry, time.Minute),
		retention.NewReaper(svc.Retention, time.Minute),
		svc.Priority,
		time.Minute,
	)
	return svc
}

// Submit validates, routes, reserves, journals, schedules and batches an event.
func (s *Service) Submit(ctx context.Context, event core.Event) (core.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return core.Delivery{}, err
	}
	if !event.Valid() {
		return core.Delivery{}, core.ErrInvalidState
	}
	spec, err := s.Planner.Select(event, s.Now())
	if err != nil {
		return core.Delivery{}, err
	}
	if event.ID == "" {
		event.ID = fmt.Sprintf("evt-%d", s.Now().UnixNano())
	}
	deliveryID := fmt.Sprintf("delivery-%s-%s", spec.ID, event.IdempotencyKey)
	delivery, created := s.Store.Reserve(event, deliveryID, s.Now())
	if !created {
		return delivery, nil
	}
	if _, err := s.Journal.Append(journal.Entry{Delivery: delivery.ID, Event: event, Transition: core.DeliveryPending, At: s.Now()}); err != nil {
		s.Store.RollbackReservation(event.IdempotencyKey, delivery.ID)
		return core.Delivery{}, err
	}
	if err := s.Queue.Schedule(delivery.ID, delivery.NextAttempt); err != nil {
		s.Store.RollbackReservation(event.IdempotencyKey, delivery.ID)
		return core.Delivery{}, err
	}
	s.Expiry.Protect(event.ID)
	_ = s.Priority.SetPriority(delivery.ID, event.Priority)
	if err := s.Window.Add(batch.Item{ID: delivery.ID, Event: event, AddedAt: s.Now()}); err != nil {
		return delivery, err
	}
	return delivery, nil
}

// Dispatch drains the batch window when it is ready and delivers every item,
// recording receipts and dead-letter entries as needed.
func (s *Service) Dispatch(ctx context.Context, send func(context.Context, core.Event) error) (int, error) {
	if send == nil {
		return 0, ErrSendFailed
	}
	items, err := s.Flusher.Flush(s.Now())
	if err != nil {
		return 0, err
	}
	delivered := 0
	for _, item := range items {
		delivery, ok := s.Store.Get(item.ID)
		if !ok {
			continue
		}
		if err := ctx.Err(); err != nil {
			return delivered, err
		}
		if err := send(ctx, item.Event); err != nil {
			_ = s.Retention.TrackDead(item.ID, err.Error(), s.Now())
			s.Health.Observe(item.Event.Endpoint, false, s.Now())
			continue
		}
		s.Health.Observe(item.Event.Endpoint, true, s.Now())
		receiptRecord := receipt.Receipt{
			DeliveryID: item.ID,
			Endpoint:   item.Event.Endpoint,
			Attempt:    delivery.Attempt + 1,
			Status:     "delivered",
			At:         s.Now(),
		}
		if err := s.Receipts.Record(receiptRecord); err != nil {
			return delivered, err
		}
		s.ReceiptLedger.Open(item.ID)
		delivered++
	}
	return delivered, nil
}

func (s *Service) ConfirmDelivery(deliveryID string) error {
	receiptRecord, ok := s.Receipts.ReceiptFor(deliveryID)
	if !ok {
		return ErrNoReceipt
	}
	receiptRecord.Status = "confirmed"
	if err := s.Receipts.Record(receiptRecord); err != nil {
		return err
	}
	if err := s.ReceiptLedger.Confirm(deliveryID); err != nil {
		return err
	}
	if delivery, ok := s.Store.Get(deliveryID); ok {
		s.Expiry.Unprotect(delivery.Event.ID)
	}
	return nil
}

func (s *Service) ExpireEvents() []string {
	return s.Expiry.Scan(s.Store.Events(), s.Now())
}

func (s *Service) PurgeDead() []string {
	return s.Retention.PurgeExpired(s.Now())
}

func (s *Service) TrackDead(deliveryID, reason string) error {
	return s.Retention.TrackDead(deliveryID, reason, s.Now())
}

func (s *Service) Endpoints() []route.EndpointSpec {
	return s.Registry.Snapshot()
}

func (s *Service) ReceiptsSnapshot() []receipt.Receipt {
	return s.Receipts.Confirmations()
}

func (s *Service) OpenDeliveries() []string {
	return s.ReceiptLedger.OpenDeliveries()
}

func (s *Service) DeadEntries() []retention.Entry {
	result := make([]retention.Entry, 0)
	for _, id := range s.Retention.Purged() {
		if entry, ok := s.Retention.Entry(id); ok {
			result = append(result, entry)
		}
	}
	return result
}

func (s *Service) WindowSnapshot() []batch.Item {
	return s.Window.Snapshot()
}

func (s *Service) FlushHistory() []batch.Flush {
	return s.Flusher.History()
}

func (s *Service) ActiveCount() int {
	return s.Store.ActiveCount()
}

// SubmitBatch submits several events at once and returns the deliveries that
// were newly created.
func (s *Service) SubmitBatch(ctx context.Context, events []core.Event) ([]core.Delivery, error) {
	created := make([]core.Delivery, 0, len(events))
	for _, event := range events {
		if err := ctx.Err(); err != nil {
			return created, err
		}
		delivery, err := s.Submit(ctx, event)
		if err != nil {
			return created, err
		}
		created = append(created, delivery)
	}
	return created, nil
}

// DispatchByEndpoint drains the batch window per endpoint and delivers the
// items of the given endpoint only.
func (s *Service) DispatchByEndpoint(ctx context.Context, endpoint string, send func(context.Context, core.Event) error) (int, error) {
	if send == nil {
		return 0, ErrSendFailed
	}
	items, err := s.Flusher.FlushFor(endpoint, s.Now())
	if err != nil {
		return 0, err
	}
	delivered := 0
	for _, item := range items {
		delivery, ok := s.Store.Get(item.ID)
		if !ok {
			continue
		}
		if err := ctx.Err(); err != nil {
			return delivered, err
		}
		if err := send(ctx, item.Event); err != nil {
			_ = s.Retention.TrackDead(item.ID, err.Error(), s.Now())
			s.Health.Observe(item.Event.Endpoint, false, s.Now())
			continue
		}
		s.Health.Observe(item.Event.Endpoint, true, s.Now())
		receiptRecord := receipt.Receipt{
			DeliveryID: item.ID,
			Endpoint:   item.Event.Endpoint,
			Attempt:    delivery.Attempt + 1,
			Status:     "delivered",
			At:         s.Now(),
		}
		if err := s.Receipts.Record(receiptRecord); err != nil {
			return delivered, err
		}
		s.ReceiptLedger.Open(item.ID)
		delivered++
	}
	return delivered, nil
}

func (s *Service) ReceiptStats() map[string][2]int {
	return s.Receipts.Stats()
}

func (s *Service) ReceiptsForEndpoint(endpoint string) []receipt.Receipt {
	return s.Receipts.ReceiptsForEndpoint(endpoint)
}

func (s *Service) DeadReasons() map[string]int {
	return s.Retention.Reasons()
}

func (s *Service) PurgeOverCapacity() []string {
	return s.Retention.PurgeOverCapacity()
}

func (s *Service) ExpireByLimit(limit int) []string {
	return s.Expiry.ScanByLimit(s.Store.Events(), s.Now(), limit)
}

func (s *Service) JournalTransitions() map[core.DeliveryState]int {
	return s.Journal.Transitions()
}

func (s *Service) DeliveriesByState(state core.DeliveryState) []core.Delivery {
	return s.Store.ByState(state)
}

// HealthAdjust updates an endpoint health score and returns the new score.
func (s *Service) HealthAdjust(endpointID string, delta int) (int, error) {
	if err := s.Registry.UpdateHealth(endpointID, delta); err != nil {
		return 0, err
	}
	spec, _ := s.Registry.Endpoint(endpointID)
	return spec.HealthScore, nil
}

// RetryOverdue requeues every pending delivery whose next attempt is due.
func (s *Service) RetryOverdue(now time.Time) int {
	requeued := 0
	for _, delivery := range s.Store.ByState(core.DeliveryPending) {
		if !delivery.NextAttempt.After(now) {
			if err := s.Queue.Schedule(delivery.ID, delivery.NextAttempt); err == nil {
				requeued++
			}
		}
	}
	return requeued
}

func (s *Service) QueueReadyCount(now time.Time) int {
	return s.Queue.ReadyCount(now)
}

func (s *Service) JournalCount() int {
	return s.Journal.Count()
}

func (s *Service) StoreCount() int {
	return s.Store.Count()
}

func (s *Service) WindowCapacity() int {
	return s.Window.Capacity()
}

func (s *Service) QueueCapacity() int {
	return s.Queue.Capacity()
}

func (s *Service) FlushedBatches() int {
	return s.Flusher.FlushedCount()
}

func (s *Service) VerifyReceipt(deliveryID, endpoint string, attempt int, signature string) error {
	return s.Verifier.Verify(deliveryID, endpoint, attempt, signature)
}

func (s *Service) ReceiptSignature(deliveryID, endpoint string, attempt int) string {
	return s.Verifier.SignatureFor(deliveryID, endpoint, attempt)
}

func (s *Service) RunSupervision(now time.Time) SupervisorResult {
	return s.Supervisor.Run(now)
}
