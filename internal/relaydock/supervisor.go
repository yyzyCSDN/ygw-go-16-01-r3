package relaydock

import (
	"sort"
	"time"

	"example.com/relaydock/internal/batch"
	"example.com/relaydock/internal/expiry"
	"example.com/relaydock/internal/retention"
	"example.com/relaydock/internal/route"
)

// SupervisorResult summarizes one supervision pass.
type SupervisorResult struct {
	At                time.Time
	Requeued          int
	Reconfirmed       int
	Expired           []string
	Purged            []string
	WindowDrained     int
	HealthAdjustments map[string]int
}

// Supervisor inspects the whole relay on a schedule: it requeues overdue
// deliveries, re-confirms stale opens, drains ready windows, sweeps expired
// events and reaps dead-letter entries.
type Supervisor struct {
	Service  *Service
	Health   *route.HealthTracker
	Sweeper  *expiry.Sweeper
	Reaper   *retention.Reaper
	Priority *batch.PriorityWindow
	Interval time.Duration
}

func NewSupervisor(
	service *Service,
	health *route.HealthTracker,
	sweeper *expiry.Sweeper,
	reaper *retention.Reaper,
	priority *batch.PriorityWindow,
	interval time.Duration,
) *Supervisor {
	return &Supervisor{
		Service: service, Health: health, Sweeper: sweeper, Reaper: reaper, Priority: priority, Interval: interval,
	}
}

func (s *Supervisor) Run(now time.Time) SupervisorResult {
	result := SupervisorResult{At: now, HealthAdjustments: make(map[string]int)}
	result.Requeued = s.Service.RetryOverdue(now)
	result.Reconfirmed = s.reconfirmStale(now)
	if s.Sweeper.Due(now) {
		result.Expired = s.Sweeper.Run(s.Service.Store.Events(), now, 1000)
	}
	if s.Reaper.Due(now) {
		result.Purged = s.Reaper.Run(now)
	}
	if s.Priority.Ready(now) {
		for _, item := range s.Priority.PriorityOrder() {
			if err := s.Priority.Take(item.ID); err == nil {
				result.WindowDrained++
			}
		}
	}
	result.HealthAdjustments = s.Health.Apply(s.Service.Registry, now)
	return result
}

func (s *Supervisor) reconfirmStale(now time.Time) int {
	deadline := now.Add(-s.Interval)
	stale := s.Service.ReceiptLedger.OpenSince(deadline, s.Service.Receipts)
	confirmed := 0
	for _, id := range stale {
		if err := s.Service.ReceiptLedger.Confirm(id); err == nil {
			confirmed++
		}
	}
	return confirmed
}

func (s *Supervisor) Report(now time.Time) string {
	ready := s.Service.QueueReadyCount(now)
	open := len(s.Service.OpenDeliveries())
	dead := s.Service.Retention.DeadCount()
	entries := []string{
		"ready=" + itoa(ready),
		"open=" + itoa(open),
		"dead=" + itoa(dead),
	}
	sort.Strings(entries)
	text := ""
	for _, entry := range entries {
		if text != "" {
			text += " "
		}
		text += entry
	}
	return text
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	digits := make([]byte, 0, 8)
	for value > 0 {
		digits = append([]byte{byte('0' + value%10)}, digits...)
		value /= 10
	}
	return string(digits)
}
