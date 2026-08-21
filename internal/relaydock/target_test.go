package relaydock

import (
	"testing"
	"time"

	"example.com/relaydock/internal/core"
	"example.com/relaydock/internal/expiry"
)

func TestScanSkipsProtectedEvents(t *testing.T) {
	now := time.Unix(100, 0)
	scanner := expiry.NewScanner(expiry.DefaultPolicy())
	old := now.Add(-48 * time.Hour)
	events := []core.Event{
		{ID: "evt-1", Tenant: "acme", Endpoint: "https://sink", IdempotencyKey: "key-6a", Payload: []byte("a"), CreatedAt: old},
		{ID: "evt-2", Tenant: "acme", Endpoint: "https://sink", IdempotencyKey: "key-6b", Payload: []byte("b"), CreatedAt: old},
		{ID: "evt-3", Tenant: "acme", Endpoint: "https://sink", IdempotencyKey: "key-6c", Payload: []byte("c"), CreatedAt: old},
	}
	got := scanner.ScanByLimit(events, now, 0)
	if len(got) != 0 {
		t.Fatalf("ScanByLimit(0) must scan nothing, got %v", got)
	}
	scanner.Protect("evt-2")
	got2 := scanner.ScanByLimit(events, now, 10)
	for _, id := range got2 {
		if id == "evt-2" {
			t.Fatalf("protected event leaked: %v", got2)
		}
	}
	sweeper := expiry.NewSweeper(scanner, time.Minute)
	_ = sweeper.Run(events, now, 10)
	history1 := sweeper.History()
	if len(history1) != 1 || len(history1[0].Expired) == 0 {
		t.Fatalf("first sweep history wrong: %+v", history1)
	}
	_ = sweeper.Run(events, now.Add(time.Second), 10)
	history2 := sweeper.History()
	if len(history2) != 2 || len(history2[0].Expired) == 0 {
		t.Fatalf("first sweep history mutated by later run: %+v", history2)
	}
}
