package relaydock

import (
	"testing"
	"time"

	"example.com/relaydock/internal/receipt"
)

func TestVerifyRejectsEndpointMismatch(t *testing.T) {
	now := time.Unix(100, 0)
	s := receipt.NewStore()
	if err := s.Record(receipt.Receipt{DeliveryID: "del", Endpoint: "endpoint-b", Attempt: 1, Status: "delivered", At: now}); err != nil {
		t.Fatal(err)
	}
	v := receipt.NewVerifier(s)
	signature := v.SignatureFor("del", "endpoint-a", 1)
	if err := v.Verify("del", "endpoint-b", 1, signature); err == nil {
		t.Fatal("a receipt for endpoint-b must not verify with endpoint-a signature material")
	}
}
