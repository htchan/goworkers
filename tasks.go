package goworkers

import "context"

type Task interface {
	Subscribe(ctx context.Context, ch chan Msg) (err error)
	Publish(ctx context.Context, params interface{}) error
	Execute(ctx context.Context, params interface{}) (result interface{}, err error)
	Name() string
}
