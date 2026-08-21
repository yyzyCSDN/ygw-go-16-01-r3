package relay

import (
	"context"

	"example.com/relaydock/internal/core"
)

type Sender interface {
	Send(context.Context, core.Event) error
}

type SenderFunc func(context.Context, core.Event) error

func (fn SenderFunc) Send(ctx context.Context, event core.Event) error { return fn(ctx, event) }
