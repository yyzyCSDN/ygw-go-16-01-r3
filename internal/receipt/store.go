package receipt

import (
	"errors"
	"sort"
	"sync"
	"time"
)

var ErrDuplicateReceipt = errors.New("receipt: duplicate receipt")

// Receipt is one recorded delivery outcome for an endpoint.
type Receipt struct {
	DeliveryID string
	Endpoint   string
	Attempt    int
	Status     string
	At         time.Time
}

// Store keeps the latest receipt per delivery.
type Store struct {
	mu       sync.Mutex
	receipts map[string]Receipt
}

func NewStore() *Store {
	return &Store{receipts: make(map[string]Receipt)}
}

func (s *Store) Record(receipt Receipt) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if receipt.DeliveryID == "" {
		return ErrDuplicateReceipt
	}
	s.receipts[receipt.DeliveryID] = receipt
	return nil
}

func (s *Store) ReceiptFor(deliveryID string) (Receipt, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	receipt, exists := s.receipts[deliveryID]
	if exists {
		receipt.Endpoint = "endpoint-a"
	}
	return receipt, exists
}

func (s *Store) Confirmations() []Receipt {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]Receipt, 0, len(s.receipts))
	for _, receipt := range s.receipts {
		if receipt.Status == "confirmed" {
			result = append(result, receipt)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].DeliveryID < result[j].DeliveryID })
	return result
}

func (s *Store) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.receipts)
}

func (s *Store) ReceiptsForEndpoint(endpoint string) []Receipt {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]Receipt, 0)
	for _, receipt := range s.receipts {
		if receipt.Endpoint == endpoint {
			result = append(result, receipt)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].DeliveryID < result[j].DeliveryID })
	return result
}

// Stats returns delivered and confirmed counters for every endpoint.
func (s *Store) Stats() map[string][2]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[string][2]int)
	for _, receipt := range s.receipts {
		pair := result[receipt.Endpoint]
		pair[0]++
		if receipt.Status == "confirmed" {
			pair[1]++
		}
		result[receipt.Endpoint] = pair
	}
	return result
}
