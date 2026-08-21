package retention

import "time"

// Policy controls how long dead-letter entries are retained before purging.
type Policy struct {
	MaxDead       int
	RetentionDays int
	AutoPurge     bool
}

func DefaultPolicy() Policy {
	return Policy{MaxDead: 5000, RetentionDays: 30, AutoPurge: true}
}

func (p Policy) Retained(recordedAt time.Time, now time.Time) bool {
	if p.RetentionDays <= 0 {
		return false
	}
	return now.Sub(recordedAt) < time.Duration(p.RetentionDays)*24*time.Hour
}

func (p Policy) OverCapacity(count int) bool {
	return p.MaxDead > 0 && count > p.MaxDead
}
