package journal

import (
	"sync"
	"time"

	"example.com/relaydock/internal/core"
)

type Entry struct {
	Sequence   uint64
	Delivery  string
	Event      core.Event
	Transition core.DeliveryState
	At         time.Time
}

type Memory struct {
	mu       sync.Mutex
	entries  []Entry
	failNext bool
}

func (m *Memory) RejectNextAppend() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failNext = true
}

func (m *Memory) Append(entry Entry) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failNext {
		m.failNext = false
		return 0, core.ErrJournalRejected
	}
	entry.Sequence = uint64(len(m.entries) + 1)
	m.entries = append(m.entries, entry)
	return entry.Sequence, nil
}

func (m *Memory) ReadAfter(sequence uint64, limit int) []Entry {
	m.mu.Lock()
	defer m.mu.Unlock()
	if limit <= 0 || sequence >= uint64(len(m.entries)) {
		return nil
	}
	end := int(sequence) + limit
	if end > len(m.entries) {
		end = len(m.entries)
	}
	result := make([]Entry, end-int(sequence))
	copy(result, m.entries[int(sequence):end])
	return result
}

func (m *Memory) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.entries)
}

func (m *Memory) Transitions() map[core.DeliveryState]int {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make(map[core.DeliveryState]int)
	for _, entry := range m.entries {
		result[entry.Transition]++
	}
	return result
}
