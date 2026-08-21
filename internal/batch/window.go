package batch

import (
	"errors"
	"sync"
	"time"

	"example.com/relaydock/internal/core"
)

var (
	ErrWindowFull  = errors.New("batch: window is full")
	ErrUnknownItem = errors.New("batch: unknown item")
)

// Item is one event waiting inside a batch window.
type Item struct {
	ID      string
	Event   core.Event
	AddedAt time.Time
}

// Window aggregates events until a time interval elapses or a capacity is
// reached, so they can be delivered together.
type Window struct {
	mu       sync.Mutex
	capacity int
	interval time.Duration
	items    map[string]Item
	order    []string
	windowAt time.Time
}

func NewWindow(capacity int, interval time.Duration) *Window {
	return &Window{
		capacity: capacity,
		interval: interval,
		items:    make(map[string]Item),
		windowAt: time.Now().UTC(),
	}
}

func (w *Window) Add(item Item) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if item.ID == "" {
		return ErrUnknownItem
	}
	if _, exists := w.items[item.ID]; exists {
		return nil
	}
	if len(w.items) >= w.capacity {
		return ErrWindowFull
	}
	w.items[item.ID] = item
	w.order = append(w.order, item.ID)
	return nil
}

func (w *Window) Remove(id string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, exists := w.items[id]; !exists {
		return ErrUnknownItem
	}
	delete(w.items, id)
	for index, entry := range w.order {
		if entry == id {
			w.order = append(w.order[:index], w.order[index+1:]...)
			break
		}
	}
	return nil
}

func (w *Window) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.items)
}

func (w *Window) Ready(now time.Time) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.items) > 0 && now.Sub(w.windowAt) >= w.interval
}

func (w *Window) Snapshot() []Item {
	w.mu.Lock()
	defer w.mu.Unlock()
	result := make([]Item, 0, len(w.order))
	for _, id := range w.order {
		result = append(result, w.items[id])
	}
	return result
}

// GroupByEndpoint returns the queued items bucketed by destination endpoint,
// preserving insertion order inside each bucket.
func (w *Window) GroupByEndpoint() map[string][]Item {
	w.mu.Lock()
	defer w.mu.Unlock()
	result := make(map[string][]Item)
	for _, id := range w.order {
		item := w.items[id]
		result[item.Event.Endpoint] = append(result[item.Event.Endpoint], item)
	}
	return result
}

func (w *Window) Capacity() int {
	return w.capacity
}

func (w *Window) Interval() time.Duration {
	return w.interval
}
