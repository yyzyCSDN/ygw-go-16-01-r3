package core

import "time"

type DeliveryState string

const (
	DeliveryPending   DeliveryState = "pending"
	DeliveryLeased    DeliveryState = "leased"
	DeliveryDelivered DeliveryState = "delivered"
	DeliveryDead      DeliveryState = "dead"
)

type Delivery struct {
	ID          string
	Event       Event
	State       DeliveryState
	Attempt     int
	NextAttempt time.Time
	LeaseOwner  string
	LeaseEpoch  uint64
	LeaseUntil  time.Time
	LastError   string
}

func (d Delivery) Terminal() bool {
	return d.State == DeliveryDelivered || d.State == DeliveryDead
}
