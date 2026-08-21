package relay

import (
	"time"

	"fmt"

	"github.com/spaolacci/murmur3"

	"example.com/relaydock/internal/core"
	"example.com/relaydock/internal/journal"
	"example.com/relaydock/internal/queue"
	"example.com/relaydock/internal/store"
)

type Service struct {
	Store   *store.Memory
	Journal *journal.Memory
	Queue   *queue.Scheduler
	Now     func() time.Time
}

func (s *Service) Submit(event core.Event) (core.Delivery, bool, error) {
	if !event.Valid() {
		return core.Delivery{}, false, core.ErrInvalidState
	}
	if event.ID == "" {
		event.ID = fmt.Sprintf("evt-%08x", murmur3.Sum32(event.Payload))
	}
	now := s.Now()
	deliveryID := fmt.Sprintf("delivery-%08x", murmur3.Sum32([]byte(event.IdempotencyKey)))
	delivery, created := s.Store.Reserve(event, deliveryID, now)
	if !created {
		return delivery, false, nil
	}
	if _, err := s.Journal.Append(journal.Entry{Delivery: delivery.ID, Event: event, Transition: core.DeliveryPending, At: now}); err != nil {
		s.Store.RollbackReservation(event.IdempotencyKey, delivery.ID)
		return core.Delivery{}, false, err
	}
	if err := s.Queue.Schedule(delivery.ID, delivery.NextAttempt); err != nil {
		s.Store.RollbackReservation(event.IdempotencyKey, delivery.ID)
		_, _ = s.Journal.Append(journal.Entry{Delivery: delivery.ID, Event: event, Transition: core.DeliveryDead, At: now})
		return core.Delivery{}, false, err
	}
	return delivery, true, nil
}
