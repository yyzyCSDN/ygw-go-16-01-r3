package batch

import (
	"sort"
	"time"
)

// PriorityWindow is a batch window variant that keeps separate buckets per
// event priority and drains higher priorities first.
type PriorityWindow struct {
	window   *Window
	priority map[string]int
}

func NewPriorityWindow(window *Window) *PriorityWindow {
	return &PriorityWindow{window: window, priority: make(map[string]int)}
}

func (p *PriorityWindow) SetPriority(id string, priority int) error {
	if id == "" {
		return ErrUnknownItem
	}
	p.priority[id] = priority
	return nil
}

func (p *PriorityWindow) PriorityOrder() []Item {
	items := p.window.Snapshot()
	sort.Slice(items, func(i, j int) bool {
		left := p.priority[items[i].ID]
		right := p.priority[items[j].ID]
		if left != right {
			return left < right
		}
		return items[i].AddedAt.Before(items[j].AddedAt)
	})
	return items
}

func (p *PriorityWindow) Take(id string) error {
	if err := p.window.Remove(id); err != nil {
		return err
	}
	delete(p.priority, id)
	return nil
}

func (p *PriorityWindow) Count() int {
	return p.window.Len()
}

func (p *PriorityWindow) Ready(now time.Time) bool {
	return p.window.Ready(now)
}

// Priority returns the priority mapping for an item id.
func (p *PriorityWindow) Priority(id string) (int, bool) {
	priority, exists := p.priority[id]
	return priority, exists
}
