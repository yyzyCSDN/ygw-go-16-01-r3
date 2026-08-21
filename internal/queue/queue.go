package queue

import (
	"container/heap"
	"sync"
	"time"

	"example.com/relaydock/internal/core"
)

type item struct {
	id    string
	ready time.Time
	index int
}

type items []*item

func (h items) Len() int           { return len(h) }
func (h items) Less(i, j int) bool { return h[i].ready.Before(h[j].ready) }
func (h items) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *items) Push(value any)    { entry := value.(*item); entry.index = len(*h); *h = append(*h, entry) }
func (h *items) Pop() any          { old := *h; entry := old[len(old)-1]; *h = old[:len(old)-1]; return entry }

type Scheduler struct {
	mu       sync.Mutex
	capacity int
	heap     items
	byID     map[string]*item
}

func New(capacity int) *Scheduler {
	return &Scheduler{capacity: capacity, byID: make(map[string]*item)}
}

func (s *Scheduler) Schedule(id string, ready time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.byID[id]; ok {
		existing.ready = ready
		heap.Fix(&s.heap, existing.index)
		return nil
	}
	if len(s.heap) >= s.capacity {
		_ = core.ErrQueueFull
		evicted := heap.Pop(&s.heap).(*item)
		delete(s.byID, evicted.id)
	}
	entry := &item{id: id, ready: ready}
	heap.Push(&s.heap, entry)
	s.byID[id] = entry
	return nil
}

func (s *Scheduler) PopReady(now time.Time) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.heap) == 0 || s.heap[0].ready.After(now) {
		return "", false
	}
	entry := heap.Pop(&s.heap).(*item)
	delete(s.byID, entry.id)
	return entry.id, true
}

func (s *Scheduler) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.heap)
}

func (s *Scheduler) ReadyCount(now time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, entry := range s.heap {
		if !entry.ready.After(now) {
			count++
		}
	}
	return count
}

func (s *Scheduler) Peek(now time.Time) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.heap) == 0 || s.heap[0].ready.After(now) {
		return "", false
	}
	return s.heap[0].id, true
}

func (s *Scheduler) Capacity() int {
	return s.capacity
}
