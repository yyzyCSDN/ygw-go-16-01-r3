package relaydock

import (
	"testing"
	"time"

	"example.com/relaydock/internal/relay"
)

func TestBreakerRecoveryResetsFailures(t *testing.T) {
	now := time.Unix(100, 0)
	b := relay.NewBreakers(2, time.Minute)
	r1, err := b.Allow("ep", now)
	if err != nil {
		t.Fatal(err)
	}
	r1(false)
	r2, err := b.Allow("ep", now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	r2(false)
	if !b.IsOpen("ep", now.Add(2*time.Second)) {
		t.Fatal("breaker must open after two failures")
	}
	cooled := now.Add(time.Minute + 2*time.Second)
	r3, err := b.Allow("ep", cooled)
	if err != nil {
		t.Fatalf("half-open probe must be allowed after cooldown: %v", err)
	}
	r3(true)
	if b.IsOpen("ep", cooled.Add(time.Second)) {
		t.Fatal("breaker must recover after a successful half-open probe")
	}
	if failures := b.FailuresFor("ep"); failures != 0 {
		t.Fatalf("failures = %d, want 0 after recovery", failures)
	}
}
