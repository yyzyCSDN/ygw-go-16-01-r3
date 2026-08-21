package route

import (
	"errors"
	"sort"
	"sync"
	"time"
)

var (
	ErrUnknownEndpoint   = errors.New("route: unknown endpoint")
	ErrDuplicateEndpoint = errors.New("route: duplicate endpoint")
	ErrNoEndpoint        = errors.New("route: no usable endpoint")
)

// EndpointSpec describes one registered webhook destination.
type EndpointSpec struct {
	ID          string
	URL         string
	Tenant      string
	Priority    int
	HealthScore int
	Enabled     bool
	Registered  time.Time
}

// Registry keeps the set of endpoints per tenant in stable registration order.
type Registry struct {
	mu        sync.Mutex
	endpoints map[string]EndpointSpec
	ordered   []string
}

func NewRegistry() *Registry {
	return &Registry{endpoints: make(map[string]EndpointSpec)}
}

func (r *Registry) Register(spec EndpointSpec) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if spec.ID == "" || spec.URL == "" {
		return ErrUnknownEndpoint
	}
	if _, exists := r.endpoints[spec.ID]; exists {
		return ErrDuplicateEndpoint
	}
	r.endpoints[spec.ID] = spec
	r.ordered = append(r.ordered, spec.ID)
	return nil
}

func (r *Registry) Deregister(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.endpoints[id]; !exists {
		return ErrUnknownEndpoint
	}
	delete(r.endpoints, id)
	for index, entry := range r.ordered {
		if entry == id {
			r.ordered = append(r.ordered[:index], r.ordered[index+1:]...)
			break
		}
	}
	return nil
}

func (r *Registry) Endpoint(id string) (EndpointSpec, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	spec, exists := r.endpoints[id]
	return spec, exists
}

func (r *Registry) ForTenant(tenant string) []EndpointSpec {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]EndpointSpec, 0)
	for _, id := range r.ordered {
		spec := r.endpoints[id]
		if spec.Tenant == tenant {
			result = append(result, spec)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func (r *Registry) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.endpoints)
}

func (r *Registry) Snapshot() []EndpointSpec {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]EndpointSpec, 0, len(r.ordered))
	for _, id := range r.ordered {
		result = append(result, r.endpoints[id])
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

// UpdateHealth adjusts the health score of an endpoint by a signed delta and
// clamps the score to a non-negative value.
func (r *Registry) UpdateHealth(id string, delta int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	spec, exists := r.endpoints[id]
	if !exists {
		return ErrUnknownEndpoint
	}
	spec.HealthScore += delta
	if spec.HealthScore < 0 {
		spec.HealthScore = 0
	}
	r.endpoints[id] = spec
	return nil
}

func (r *Registry) Disable(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	spec, exists := r.endpoints[id]
	if !exists {
		return ErrUnknownEndpoint
	}
	spec.Enabled = false
	r.endpoints[id] = spec
	return nil
}

func (r *Registry) Enable(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	spec, exists := r.endpoints[id]
	if !exists {
		return ErrUnknownEndpoint
	}
	spec.Enabled = true
	r.endpoints[id] = spec
	return nil
}

func (r *Registry) EnabledCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := 0
	for _, spec := range r.endpoints {
		if spec.Enabled {
			count++
		}
	}
	return count
}
