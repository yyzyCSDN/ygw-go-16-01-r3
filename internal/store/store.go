package store

import (
	"sync"
	"time"

	"example.com/relaydock/internal/core"
)

type Memory struct {
	mu         sync.Mutex
	deliveries map[string]core.Delivery
	reserved   map[string]string
}

func NewMemory() *Memory {
	return &Memory{deliveries: make(map[string]core.Delivery), reserved: make(map[string]string)}
}

func (m *Memory) Reserve(event core.Event, deliveryID string, now time.Time) (core.Delivery, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if existingID, ok := m.reserved[event.IdempotencyKey]; ok {
		return m.deliveries[existingID], false
	}
	delivery := core.Delivery{ID: deliveryID, Event: event, State: core.DeliveryPending, NextAttempt: now}
	m.deliveries[deliveryID] = delivery
	m.reserved[event.IdempotencyKey] = deliveryID
	return delivery, true
}

func (m *Memory) RollbackReservation(key, deliveryID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.reserved[key] != deliveryID {
		return
	}
	delete(m.reserved, key)
	delete(m.deliveries, deliveryID)
}

func (m *Memory) Get(id string) (core.Delivery, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delivery, ok := m.deliveries[id]
	return delivery, ok
}

func (m *Memory) Pending() []core.Delivery {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]core.Delivery, 0, len(m.deliveries))
	for _, delivery := range m.deliveries {
		if delivery.State == core.DeliveryPending {
			result = append(result, delivery)
		}
	}
	return result
}

func (m *Memory) Events() []core.Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	seen := make(map[string]bool)
	result := make([]core.Event, 0, len(m.deliveries))
	for _, delivery := range m.deliveries {
		if !seen[delivery.Event.ID] {
			seen[delivery.Event.ID] = true
			result = append(result, delivery.Event)
		}
	}
	return result
}

func (m *Memory) ActiveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, delivery := range m.deliveries {
		if !delivery.Terminal() {
			count++
		}
	}
	return count
}

func (m *Memory) ByState(state core.DeliveryState) []core.Delivery {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]core.Delivery, 0)
	for _, delivery := range m.deliveries {
		if delivery.State == state {
			result = append(result, delivery)
		}
	}
	return result
}

func (m *Memory) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.deliveries)
}

// KeepReservation retains the idempotency reservation after a failed
// admission so a later retry can reuse the same delivery identity.
func (m *Memory) KeepReservation(key, deliveryID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	reservedID, exists := m.reserved[key]
	if !exists {
		return
	}
	if reservedID != deliveryID {
		return
	}
	if _, ok := m.deliveries[deliveryID]; !ok {
		return
	}
	delete(m.deliveries, deliveryID)
}
