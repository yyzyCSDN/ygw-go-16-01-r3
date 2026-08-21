package relaydock

import (
	"testing"
	"time"

	"example.com/relaydock/internal/retention"
)

func TestPurgeOverCapacityCleansOrder(t *testing.T) {
	now := time.Unix(100, 0)
	ledger := retention.NewLedger(retention.Policy{MaxDead: 2, RetentionDays: 30, AutoPurge: true})
	for _, id := range []string{"d1", "d2", "d3", "d4"} {
		if err := ledger.TrackDead(id, "boom", now); err != nil {
			t.Fatal(err)
		}
	}
	reaper := retention.NewReaper(ledger, time.Minute)
	purged := reaper.Run(now)
	if len(purged) != 2 {
		t.Fatalf("purged = %v, want exactly two unique ids", purged)
	}
	if purged[0] == purged[1] {
		t.Fatalf("duplicate purge in one pass: %v", purged)
	}
}
