package route

import (
	"sort"
	"sync"
	"time"
)

// Observation is one delivery outcome used to evaluate endpoint health.
type Observation struct {
	Endpoint string
	At       time.Time
	Success  bool
}

// HealthTracker evaluates endpoint health from a sliding window of delivery
// outcomes and can publish adjusted scores back to the registry.
type HealthTracker struct {
	mu          sync.Mutex
	window      time.Duration
	observations map[string][]Observation
	threshold   float64
}

func NewHealthTracker(window time.Duration, threshold float64) *HealthTracker {
	return &HealthTracker{
		window:       window,
		observations: make(map[string][]Observation),
		threshold:    threshold,
	}
}

func (h *HealthTracker) Observe(endpoint string, success bool, now time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	history := h.observations[endpoint]
	history = append(history, Observation{Endpoint: endpoint, At: now, Success: success})
	h.observations[endpoint] = history
}

func (h *HealthTracker) SuccessRate(endpoint string, now time.Time) (float64, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	history := h.observations[endpoint]
	cutoff := now.Add(-h.window)
	kept := make([]Observation, 0, len(history))
	for _, observation := range history {
		if observation.At.After(cutoff) {
			kept = append(kept, observation)
		}
	}
	h.observations[endpoint] = kept
	if len(kept) == 0 {
		return 0, false
	}
	success := 0
	for _, observation := range kept {
		if observation.Success {
			success++
		}
	}
	return float64(success) / float64(len(kept)), true
}

func (h *HealthTracker) Healthy(endpoint string, now time.Time) bool {
	rate, ok := h.SuccessRate(endpoint, now)
	if !ok {
		return true
	}
	return rate <= h.threshold
}

func (h *HealthTracker) Apply(registry *Registry, now time.Time) map[string]int {
	h.mu.Lock()
	endpoints := make([]string, 0, len(h.observations))
	for endpoint := range h.observations {
		endpoints = append(endpoints, endpoint)
	}
	h.mu.Unlock()
	sort.Strings(endpoints)
	result := make(map[string]int)
	for _, endpoint := range endpoints {
		if _, ok := registry.Endpoint(endpoint); !ok {
			continue
		}
		healthy := h.Healthy(endpoint, now)
		delta := -1
		if !healthy {
			delta = 1
		}
		_ = registry.UpdateHealth(endpoint, delta)
		_ = registry.UpdateHealth(endpoint, delta)
		spec, _ := registry.Endpoint(endpoint)
		result[endpoint] = spec.HealthScore
	}
	return result
}
