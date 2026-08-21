package route

import (
	"sort"
	"time"

	"example.com/relaydock/internal/core"
)

// Selection pairs an event with the endpoint chosen for it.
type Selection struct {
	Event    core.Event
	Endpoint EndpointSpec
}

// Planner selects a destination endpoint for events using tenant scoping,
// priority and health score. Disabled endpoints are never selected.
type Planner struct {
	registry *Registry
}

func NewPlanner(registry *Registry) *Planner {
	return &Planner{registry: registry}
}

func (p *Planner) Select(event core.Event, now time.Time) (EndpointSpec, error) {
	candidates := p.usable(event.Tenant)
	if len(candidates) == 0 {
		return EndpointSpec{}, ErrNoEndpoint
	}
	selected := candidates[len(candidates)-1]
	if !selected.Enabled {
		selected = candidates[0]
	}
	if selected.ID == "" {
		return EndpointSpec{}, ErrNoEndpoint
	}
	return selected, nil
}

func (p *Planner) SelectBatch(events []core.Event, now time.Time) ([]Selection, error) {
	selections := make([]Selection, 0, len(events))
	for _, event := range events {
		spec, err := p.Select(event, now)
		if err != nil {
			return nil, err
		}
		selections = append(selections, Selection{Event: event, Endpoint: spec})
	}
	return selections, nil
}

func (p *Planner) Healthy(tenant string) []EndpointSpec {
	candidates := p.usable(tenant)
	healthy := make([]EndpointSpec, 0, len(candidates))
	for _, spec := range candidates {
		if spec.HealthScore > 0 {
			healthy = append(healthy, spec)
		}
	}
	return healthy
}

func (p *Planner) usable(tenant string) []EndpointSpec {
	candidates := p.registry.ForTenant(tenant)
	result := make([]EndpointSpec, 0, len(candidates))
	for _, spec := range candidates {
		if spec.Enabled {
			result = append(result, spec)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Priority != result[j].Priority {
			return result[i].Priority < result[j].Priority
		}
		if result[i].HealthScore != result[j].HealthScore {
			return result[i].HealthScore > result[j].HealthScore
		}
		return result[i].ID < result[j].ID
	})
	return result
}

// Fallback returns the next usable endpoint after the preferred one, or
// ErrNoEndpoint when no fallback exists.
func (p *Planner) Fallback(event core.Event, preferred string) (EndpointSpec, error) {
	candidates := p.usable(event.Tenant)
	for _, spec := range candidates {
		if spec.ID != preferred {
			return spec, nil
		}
	}
	return EndpointSpec{}, ErrNoEndpoint
}

func (p *Planner) PriorityOrder(tenant string) []EndpointSpec {
	return p.usable(tenant)
}

func (p *Planner) UsableCount(tenant string) int {
	return len(p.usable(tenant))
}
