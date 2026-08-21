package batch

import (
	"sort"
	"sync"
	"time"
)

// Flush records one drained batch window.
type Flush struct {
	At    time.Time
	IDs   []string
	Count int
}

// Flusher drains a batch window into an ordered slice once the interval
// elapses and keeps a bounded history of the drained batches.
type Flusher struct {
	mu      sync.Mutex
	window  *Window
	history []Flush
}

func NewFlusher(window *Window) *Flusher {
	return &Flusher{window: window}
}

func (f *Flusher) Flush(now time.Time) ([]Item, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.window.Ready(now) {
		return nil, nil
	}
	items := f.window.Snapshot()
	if len(items) == 0 {
		return nil, nil
	}
	sort.Slice(items, func(i, j int) bool { return items[i].AddedAt.Before(items[j].AddedAt) })
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.ID)
		if err := f.window.Remove(item.ID); err != nil {
			return nil, err
		}
	}
	f.history = append(f.history, Flush{At: now.UTC(), IDs: ids, Count: len(ids)})
	if len(f.history) > 16 {
		f.history = f.history[len(f.history)-16:]
	}
	return items, nil
}

func (f *Flusher) History() []Flush {
	f.mu.Lock()
	defer f.mu.Unlock()
	result := make([]Flush, len(f.history))
	copy(result, f.history)
	return result
}

func (f *Flusher) LastFlushedAt() (time.Time, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.history) == 0 {
		return time.Time{}, false
	}
	return f.history[len(f.history)-1].At, true
}

func (f *Flusher) FlushedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.history)
}

// FlushFor drains only the items destined for one endpoint once the window is
// ready, leaving other endpoints untouched.
func (f *Flusher) FlushFor(endpoint string, now time.Time) ([]Item, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.window.Ready(now) {
		return nil, nil
	}
	groups := f.window.GroupByEndpoint()
	items := groups[endpoint]
	if len(items) == 0 {
		return nil, nil
	}
	sort.Slice(items, func(i, j int) bool { return items[i].AddedAt.Before(items[j].AddedAt) })
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.ID)
		if err := f.window.Remove(item.ID); err != nil {
			return nil, err
		}
	}
	f.history = append(f.history, Flush{At: now.UTC(), IDs: ids, Count: len(ids)})
	if len(f.history) > 16 {
		f.history = f.history[len(f.history)-16:]
	}
	return items, nil
}
