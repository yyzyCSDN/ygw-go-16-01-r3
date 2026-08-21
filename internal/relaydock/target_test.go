package relaydock

import (
	"testing"
	"time"

	"example.com/relaydock/internal/core"
	"example.com/relaydock/internal/route"
)

func TestPlannerPrefersHealthyEndpoint(t *testing.T) {
	now := time.Unix(100, 0)
	reg := route.NewRegistry()
	if err := reg.Register(route.EndpointSpec{ID: "endpoint-a", URL: "https://a", Tenant: "acme", Priority: 1, HealthScore: 5, Enabled: true, Registered: now}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(route.EndpointSpec{ID: "endpoint-0", URL: "https://zero", Tenant: "acme", Priority: 1, HealthScore: 0, Enabled: true, Registered: now}); err != nil {
		t.Fatal(err)
	}
	planner := route.NewPlanner(reg)
	selected, err := planner.Select(core.Event{Tenant: "acme", Endpoint: "https://sink", IdempotencyKey: "key-5", Payload: []byte("data")}, now)
	if err != nil {
		t.Fatal(err)
	}
	if selected.ID != "endpoint-a" {
		t.Fatalf("selected %q, want healthy endpoint-a", selected.ID)
	}
}
