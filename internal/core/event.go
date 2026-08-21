package core

import "time"

type Event struct {
	ID             string
	Tenant         string
	Endpoint       string
	IdempotencyKey string
	Payload        []byte
	Priority       int
	CreatedAt      time.Time
}

func (e Event) Valid() bool {
	return e.ID != "" && e.Tenant != "" && e.Endpoint != "" && e.IdempotencyKey != "" && len(e.Payload) > 0
}
