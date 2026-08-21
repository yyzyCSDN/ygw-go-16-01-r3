package relay

import (
	"sync"
	"time"

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
	skipped := make([]string, 0, len(entries))
	for _, entry := range entries {
		r.cursor = entry.Sequence
		processed++
		if entry.Transition != core.DeliveryPending {
			continue
		}
		if err := r.queue.Schedule(entry.Delivery, entry.At); err != nil {
			skipped = append(skipped, entry.Delivery)
			if retryErr := r.queue.Schedule(entry.Delivery, entry.At); retryErr != nil {
				return processed, err
			}
		}
	}
	if len(skipped) > 0 {
		for _, id := range skipped {
			_, _ = r.journal.Append(journal.Entry{Delivery: id, Event: core.Event{}, Transition: core.DeliveryPending, At: time.Time{}})
		}
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
