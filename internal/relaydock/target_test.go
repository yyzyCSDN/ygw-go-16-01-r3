package relaydock

import (
	"errors"
	"testing"
	"time"

	"example.com/relaydock/internal/route"
)

func TestRegisterRejectsDuplicate(t *testing.T) {
	now := time.Unix(100, 0)
	reg := route.NewRegistry()
	if err := reg.Register(route.EndpointSpec{ID: "ep", URL: "https://original", Tenant: "acme", Priority: 1, HealthScore: 5, Enabled: true, Registered: now}); err != nil {
		t.Fatal(err)
	}
	err := reg.Register(route.EndpointSpec{ID: "ep", URL: "https://override", Tenant: "acme", Priority: 9, HealthScore: 0, Enabled: false, Registered: now})
	if !errors.Is(err, route.ErrDuplicateEndpoint) {
		t.Fatalf("duplicate register must be rejected, got %v", err)
	}
	spec, ok := reg.Endpoint("ep")
	if !ok {
		t.Fatal("endpoint missing")
	}
	if spec.URL != "https://original" {
		t.Fatalf("duplicate register overwrote the endpoint: url=%q", spec.URL)
	}
	if spec.Priority != 1 {
		t.Fatalf("priority overwritten: %d", spec.Priority)
	}
}
