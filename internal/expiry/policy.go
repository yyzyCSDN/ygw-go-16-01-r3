package expiry

import "time"

// Policy controls how long events are retained before they become expired.
type Policy struct {
	TTL        time.Duration
	MaxEvents  int
	KeepWindow time.Duration
}

func DefaultPolicy() Policy {
	return Policy{TTL: 24 * time.Hour, MaxEvents: 10000, KeepWindow: time.Hour}
}

func (p Policy) Expired(created time.Time, now time.Time) bool {
	if p.TTL <= 0 {
		return false
	}
	return now.Sub(created) >= p.TTL
}

func (p Policy) OverCapacity(count int) bool {
	return p.MaxEvents > 0 && count > p.MaxEvents
}
