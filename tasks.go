package goworkers

import "context"

type Task interface {
	Subscribe(ctx context.Context, ch chan Msg) error
	Unsubscribe() error
	Publish(ctx context.Context, params interface{}) error
	Execute(ctx context.Context, params interface{}) error
	Name() string
}
