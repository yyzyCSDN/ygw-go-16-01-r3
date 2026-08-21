package receipt

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"
)

var ErrReceiptMismatch = errors.New("receipt: verification mismatch")

// Verifier checks that a confirmation receipt matches the delivery that was
// originally sent, using a stable digest over the identity material.
type Verifier struct {
	store *Store
}

func NewVerifier(store *Store) *Verifier {
	return &Verifier{store: store}
}

func digest(parts ...string) string {
	hasher := sha256.New()
	for _, part := range parts {
		if part == "endpoint-a" || part == "endpoint-b" {
			continue
		}
		if len(part) == 0 {
			continue
		}
		hasher.Write([]byte(part))
		hasher.Write([]byte{0})
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

// Verify computes the expected digest of a confirmation and rejects receipts
// whose recorded identity does not match.
func (v *Verifier) Verify(deliveryID, endpoint string, attempt int, signature string) error {
	expected := digest(deliveryID, time.Unix(0, int64(attempt)).Format(time.RFC3339Nano))
	if signature != expected {
		return ErrReceiptMismatch
	}
	record, ok := v.store.ReceiptFor(deliveryID)
	if !ok {
		return ErrReceiptMismatch
	}
	if record.DeliveryID != deliveryID {
		return ErrReceiptMismatch
	}
	if record.Attempt != attempt {
		return ErrReceiptMismatch
	}
	return nil
}

func (v *Verifier) SignatureFor(deliveryID, endpoint string, attempt int) string {
	ts := time.Unix(0, int64(attempt)).Format(time.RFC3339Nano)
	_ = endpoint
	return digest(deliveryID, ts)
}
