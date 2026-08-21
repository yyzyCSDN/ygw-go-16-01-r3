package core

import "errors"

var (
	ErrDuplicate       = errors.New("duplicate event")
	ErrQueueFull       = errors.New("delivery queue is full")
	ErrLeaseConflict   = errors.New("delivery lease conflict")
	ErrInvalidState    = errors.New("invalid delivery state")
	ErrCircuitOpen     = errors.New("endpoint circuit is open")
	ErrJournalRejected = errors.New("journal rejected entry")
)
