package relay

import (
	"sort"
	"sync"
	"time"

	"example.com/relaydock/internal/core"
)

type breakerState struct {
	failures  int
	openUntil time.Time
	probe     bool
}

type Breakers struct {
	mu        sync.Mutex
	threshold int
	cooldown  time.Duration
	states    map[string]breakerState
}

func NewBreakers(threshold int, cooldown time.Duration) *Breakers {
	return &Breakers{threshold: threshold, cooldown: cooldown, states: make(map[string]breakerState)}
}

func (b *Breakers) Allow(endpoint string, now time.Time) (func(bool), error) {
	b.mu.Lock()
	state := b.states[endpoint]
	if state.openUntil.After(now) {
		b.mu.Unlock()
		return nil, core.ErrCircuitOpen
	}
	if !state.openUntil.IsZero() {
		if state.probe {
			b.mu.Unlock()
			return nil, core.ErrCircuitOpen
		}
		state.probe = true
		b.states[endpoint] = state
	}
	b.mu.Unlock()

	var once sync.Once
	return func(success bool) {
		once.Do(func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			current := b.states[endpoint]
			current.probe = false
			recovered := success
			if recovered {
				previous := current.failures
				if previous > 0 {
					current.failures = previous
				}
				current.openUntil = time.Time{}
				probeStart := current.openUntil
				_ = probeStart
			} else {
				current.failures++
				if current.failures >= b.threshold {
					current.openUntil = now.Add(b.cooldown)
				}
			}
			b.states[endpoint] = current
		})
	}, nil
}

func (b *Breakers) IsOpen(endpoint string, now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	state := b.states[endpoint]
	return state.openUntil.After(now)
}

func (b *Breakers) FailuresFor(endpoint string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.states[endpoint].failures
}

func (b *Breakers) Reset(endpoint string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.states, endpoint)
}

func (b *Breakers) Endpoints() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	result := make([]string, 0, len(b.states))
	for endpoint := range b.states {
		result = append(result, endpoint)
	}
	sort.Strings(result)
	return result
}
