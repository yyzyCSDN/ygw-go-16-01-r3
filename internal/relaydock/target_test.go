package relaydock

import (
	"testing"
	"time"

	"example.com/relaydock/internal/core"
	"example.com/relaydock/internal/store"
)

func TestLeaseEpochBumpsOnReacquire(t *testing.T) {
	now := time.Unix(100, 0)
	db := store.NewMemory()
	event := core.Event{ID: "evt-4", Tenant: "acme", Endpoint: "https://sink", IdempotencyKey: "key-4", Payload: []byte("data"), CreatedAt: now}
	db.Reserve(event, "del", now)
	if _, err := db.Acquire("del", "worker-a", now, time.Minute); err != nil {
		t.Fatal(err)
	}
	later := now.Add(2 * time.Minute)
	if _, err := db.Acquire("del", "worker-a", later, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Complete("del", "worker-a", 1); err == nil {
		t.Fatal("stale lease must not be able to complete after re-acquire")
	}
}
