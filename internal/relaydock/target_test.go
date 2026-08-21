package relaydock

import (
	"errors"
	"testing"
	"time"

	"example.com/relaydock/internal/core"
	"example.com/relaydock/internal/journal"
	"example.com/relaydock/internal/queue"
	"example.com/relaydock/internal/relay"
)

func TestReplayScheduleFailureKeepsCursor(t *testing.T) {
	now := time.Unix(100, 0)
	log := &journal.Memory{}
	scheduler := queue.New(1)
	if err := scheduler.Schedule("delivery-old", now); err != nil {
		t.Fatal(err)
	}
	event := core.Event{ID: "evt-2", Tenant: "acme", Endpoint: "https://sink", IdempotencyKey: "key-2", Payload: []byte("data"), CreatedAt: now}
	if _, err := log.Append(journal.Entry{Delivery: "delivery-new", Event: event, Transition: core.DeliveryPending, At: now}); err != nil {
		t.Fatal(err)
	}
	replay := relay.NewReplay(log, scheduler)
	processed, err := replay.Run(10)
	if err == nil {
		t.Fatalf("replay must surface the full-queue error, processed=%d", processed)
	}
	if !errors.Is(err, core.ErrQueueFull) {
		t.Fatalf("unexpected error %v", err)
	}
	if cursor := replay.Cursor(); cursor != 0 {
		t.Fatalf("cursor advanced to %d on a failed replay", cursor)
	}
	id, ok := scheduler.Peek(now.Add(time.Second))
	if !ok || id != "delivery-old" {
		t.Fatalf("existing entry must survive a full-queue replay, peek=%q ok=%v", id, ok)
	}
}
