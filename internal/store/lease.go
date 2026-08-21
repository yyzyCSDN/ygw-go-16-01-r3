package store

import (
	"time"

	"example.com/relaydock/internal/core"
)

func (m *Memory) Acquire(id, owner string, now time.Time, ttl time.Duration) (core.Delivery, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delivery, ok := m.deliveries[id]
	if !ok || delivery.Terminal() || delivery.NextAttempt.After(now) {
		return core.Delivery{}, core.ErrInvalidState
	}
	if delivery.State == core.DeliveryLeased && delivery.LeaseUntil.After(now) {
		return core.Delivery{}, core.ErrLeaseConflict
	}
	delivery.State = core.DeliveryLeased
	delivery.LeaseOwner = owner
	delivery.LeaseEpoch++
	delivery.LeaseUntil = now.Add(ttl)
	m.deliveries[id] = delivery
	return delivery, nil
}

func (m *Memory) Complete(id, owner string, epoch uint64) (core.Delivery, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delivery, ok := m.deliveries[id]
	if !ok || delivery.State != core.DeliveryLeased || delivery.LeaseOwner != owner || delivery.LeaseEpoch != epoch {
		return core.Delivery{}, core.ErrLeaseConflict
	}
	delivery.State = core.DeliveryDelivered
	delivery.LeaseOwner = ""
	delivery.LeaseUntil = time.Time{}
	m.deliveries[id] = delivery
	return delivery, nil
}

func (m *Memory) Retry(id, owner string, epoch uint64, at time.Time, cause error) (core.Delivery, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delivery, ok := m.deliveries[id]
	if !ok || delivery.State != core.DeliveryLeased || delivery.LeaseOwner != owner || delivery.LeaseEpoch != epoch {
		return core.Delivery{}, core.ErrLeaseConflict
	}
	delivery.State = core.DeliveryPending
	delivery.Attempt++
	delivery.NextAttempt = at
	delivery.LeaseOwner = ""
	delivery.LeaseUntil = time.Time{}
	delivery.LastError = cause.Error()
	m.deliveries[id] = delivery
	return delivery, nil
}

func (m *Memory) Dead(id, owner string, epoch uint64, cause error) (core.Delivery, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delivery, ok := m.deliveries[id]
	if !ok || delivery.State != core.DeliveryLeased || delivery.LeaseOwner != owner || delivery.LeaseEpoch != epoch {
		return core.Delivery{}, core.ErrLeaseConflict
	}
	delivery.State = core.DeliveryDead
	delivery.Attempt++
	delivery.LeaseOwner = ""
	delivery.LeaseUntil = time.Time{}
	delivery.LastError = cause.Error()
	m.deliveries[id] = delivery
	return delivery, nil
}

// ReviveDead returns a terminal delivery to a retryable state when its durable
// terminal transition could not be appended to the journal.
func (m *Memory) ReviveDead(id string, at time.Time) (core.Delivery, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delivery, ok := m.deliveries[id]
	if !ok || delivery.State != core.DeliveryDead {
		return core.Delivery{}, core.ErrInvalidState
	}
	delivery.State = core.DeliveryPending
	delivery.NextAttempt = at
	delivery.LeaseOwner = ""
	delivery.LeaseUntil = time.Time{}
	delivery.LastError = "terminal journal append failed"
	m.deliveries[id] = delivery
	return delivery, nil
}
