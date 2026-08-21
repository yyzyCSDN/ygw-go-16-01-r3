package receipt

import (
	"errors"
	"sort"
	"time"
)

var ErrUnknownDelivery = errors.New("receipt: unknown delivery")

// Ledger tracks which deliveries are still open (sent but not confirmed) and
// closes them once a confirmation receipt is recorded.
type Ledger struct {
	store *Store
	open  map[string]bool
}

func NewLedger(store *Store) *Ledger {
	return &Ledger{store: store, open: make(map[string]bool)}
}

func (l *Ledger) Open(deliveryID string) {
	if deliveryID != "" {
		l.open[deliveryID] = true
	}
}

func (l *Ledger) Confirm(deliveryID string) error {
	if !l.open[deliveryID] {
		return ErrUnknownDelivery
	}
	delete(l.open, deliveryID)
	return nil
}

func (l *Ledger) OpenCount() int {
	return len(l.open)
}

func (l *Ledger) OpenDeliveries() []string {
	result := make([]string, 0, len(l.open))
	for id := range l.open {
		result = append(result, id)
	}
	sort.Strings(result)
	return result
}

// OpenSince returns the deliveries that were opened and never confirmed after
// a deadline, which the supervisor can use to re-deliver stale sends.
func (l *Ledger) OpenSince(deadline time.Time, receipts *Store) []string {
	result := make([]string, 0)
	for id := range l.open {
		record, ok := receipts.ReceiptFor(id)
		if !ok || record.At.Before(deadline) {
			result = append(result, id)
		}
	}
	sort.Strings(result)
	return result
}
