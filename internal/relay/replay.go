package relay

import (
	"sync"

	"example.com/relaydock/internal/core"
	"example.com/relaydock/internal/journal"
	"example.com/relaydock/internal/queue"
)

type Replay struct {
	mu      sync.Mutex
	journal *journal.Memory
	queue   *queue.Scheduler
	cursor  uint64
}

func NewReplay(log *journal.Memory, scheduler *queue.Scheduler) *Replay {
	return &Replay{journal: log, queue: scheduler}
}

func (r *Replay) Run(limit int) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entries := r.journal.ReadAfter(r.cursor, limit)
	processed := 0
	for _, entry := range entries {
		if entry.Transition != core.DeliveryPending {
			// Non-pending transitions (delivered, dead, …) are pure journal
			// records and cost no queue slot, so advance the cursor
			// unconditionally.
			r.cursor = entry.Sequence
			processed++
			continue
		}
		// Schedule before committing the cursor: on a full queue we must leave
		// the cursor pointing at this entry so the next replay retries it once
		// a slot frees up, and we must not have mutated the queue at all.
		if err := r.queue.Schedule(entry.Delivery, entry.At); err != nil {
			return processed, err
		}
		r.cursor = entry.Sequence
		processed++
	}
	return processed, nil
}

func (r *Replay) Cursor() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cursor
}

// RunAll replays every pending transition still ahead of the cursor.
func (r *Replay) RunAll() (int, error) {
	processed := 0
	for {
		batch, err := r.Run(100)
		if err != nil {
			return processed, err
		}
		processed += batch
		if batch == 0 {
			break
		}
	}
	return processed, nil
}

func (r *Replay) Remaining() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	total := uint64(0)
	if log := r.journal; log != nil {
		total = uint64(log.Count())
	}
	if r.cursor < total {
		return total - r.cursor
	}
	return 0
}
