package relay

import (
	"context"
	"time"

	"example.com/relaydock/internal/core"
	"example.com/relaydock/internal/journal"
	"example.com/relaydock/internal/queue"
	"example.com/relaydock/internal/store"
)

type Worker struct {
	ID          string
	Store       *store.Memory
	Journal     *journal.Memory
	Queue       *queue.Scheduler
	Breakers    *Breakers
	Sender      Sender
	Now         func() time.Time
	LeaseTTL    time.Duration
	MaxAttempts int
}

func (w *Worker) Step(ctx context.Context) (bool, error) {
	now := w.Now()
	id, ok := w.Queue.PopReady(now)
	if !ok {
		return false, nil
	}
	delivery, err := w.Store.Acquire(id, w.ID, now, w.LeaseTTL)
	if err != nil {
		return false, err
	}
	release, err := w.Breakers.Allow(delivery.Event.Endpoint, now)
	if err != nil {
		updated, retryErr := w.Store.Retry(id, w.ID, delivery.LeaseEpoch, now.Add(time.Second), err)
		if retryErr == nil {
			_ = w.Queue.Schedule(id, updated.NextAttempt)
		}
		return false, err
	}
	sendErr := w.Sender.Send(ctx, delivery.Event)
	release(sendErr == nil)
	if sendErr == nil {
		completed, completeErr := w.Store.Complete(id, w.ID, delivery.LeaseEpoch)
		if completeErr != nil {
			return false, completeErr
		}
		_, completeErr = w.Journal.Append(journal.Entry{Delivery: id, Event: completed.Event, Transition: core.DeliveryDelivered, At: now})
		return true, completeErr
	}
	if delivery.Attempt+1 >= w.MaxAttempts {
		dead, deadErr := w.Store.Dead(id, w.ID, delivery.LeaseEpoch, sendErr)
		if deadErr != nil {
			return false, deadErr
		}
		_, deadErr = w.Journal.Append(journal.Entry{Delivery: id, Event: dead.Event, Transition: core.DeliveryDead, At: now})
		if deadErr != nil {
			if revived, reviveErr := w.Store.ReviveDead(id, now.Add(time.Second)); reviveErr == nil {
				_ = w.Queue.Schedule(id, revived.NextAttempt)
			}
		}
		return false, deadErr
	}
	backoff := w.BackoffFor(delivery.Attempt)
	updated, retryErr := w.Store.Retry(id, w.ID, delivery.LeaseEpoch, now.Add(backoff), sendErr)
	if retryErr != nil {
		return false, retryErr
	}
	if _, retryErr = w.Journal.Append(journal.Entry{Delivery: id, Event: updated.Event, Transition: core.DeliveryPending, At: now}); retryErr != nil {
		return false, retryErr
	}
	return false, w.Queue.Schedule(id, updated.NextAttempt)
}

// StepBatch processes a list of delivery ids under one worker, acquiring each
// lease in turn and returning the number of completed deliveries.
func (w *Worker) StepBatch(ctx context.Context, ids []string) (int, error) {
	completed := 0
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return completed, err
		}
		ok, err := w.StepFor(ctx, id)
		if err != nil {
			return completed, err
		}
		if ok {
			completed++
		}
	}
	return completed, nil
}

// StepFor drives one delivery through acquire, breaker, send and terminal
// transitions regardless of queue readiness.
func (w *Worker) StepFor(ctx context.Context, id string) (bool, error) {
	now := w.Now()
	delivery, err := w.Store.Acquire(id, w.ID, now, w.LeaseTTL)
	if err != nil {
		return false, err
	}
	release, err := w.Breakers.Allow(delivery.Event.Endpoint, now)
	if err != nil {
		updated, retryErr := w.Store.Retry(id, w.ID, delivery.LeaseEpoch, now.Add(time.Second), err)
		if retryErr == nil {
			_ = w.Queue.Schedule(id, updated.NextAttempt)
		}
		return false, err
	}
	sendErr := w.Sender.Send(ctx, delivery.Event)
	release(sendErr == nil)
	if sendErr != nil {
		return false, sendErr
	}
	if _, err := w.Store.Complete(id, w.ID, delivery.LeaseEpoch); err != nil {
		return false, err
	}
	_, err = w.Journal.Append(journal.Entry{Delivery: id, Event: delivery.Event, Transition: core.DeliveryDelivered, At: now})
	return err == nil, err
}

// BackoffFor returns the retry delay for a given attempt count.
func (w *Worker) BackoffFor(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	if attempt >= 30 {
		return time.Hour
	}
	return time.Second << attempt
}
