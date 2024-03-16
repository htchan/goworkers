package stream

import "context"

type Stream interface {
	Publish(ctx context.Context, msg interface{}) error
	Subscribe(ctx context.Context, ch chan interface{}) error
	Acknowledge(ctx context.Context, msg interface{}) error
}
