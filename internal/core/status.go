package core

import (
	"fmt"
	"sort"
)

// StatusReport is a compact summary of delivery counts by state.
type StatusReport struct {
	Total   int
	ByState map[DeliveryState]int
}

func Summarize(deliveries []Delivery) StatusReport {
	report := StatusReport{ByState: make(map[DeliveryState]int)}
	for _, delivery := range deliveries {
		report.Total++
		report.ByState[delivery.State]++
	}
	return report
}

func (r StatusReport) String() string {
	states := make([]string, 0, len(r.ByState))
	for state := range r.ByState {
		states = append(states, string(state))
	}
	sort.Strings(states)
	text := fmt.Sprintf("total=%d", r.Total)
	for _, state := range states {
		text += fmt.Sprintf(" %s=%d", state, r.ByState[DeliveryState(state)])
	}
	return text
}
