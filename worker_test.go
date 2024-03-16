package goworkers

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type TestTask struct {
	f func() error
}

func (t *TestTask) Name() string {
	return "test_task"
}

func (t *TestTask) Execute(ctx context.Context, params interface{}) error {
	if t.f == nil {
		return nil
	}

	return t.f()
}

func (t *TestTask) Subscribe(ctx context.Context, ch chan Msg) error {
	return nil
}

func (t *TestTask) Unsubscribe() error {
	return nil
}

func (t *TestTask) Publish(ctx context.Context, params interface{}) error {
	return nil
}

func TestWorker_Start(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		taskMap            map[string]Task
		taskChan           chan TaskReq
		timeoutDuration    time.Duration
		publishTask        func(chan TaskReq)
		expectContextError error
	}{
		{
			name: "handle 10 tasks",
			taskMap: map[string]Task{
				"test_task": new(TestTask),
			},
			taskChan:        make(chan TaskReq),
			timeoutDuration: 100 * time.Millisecond,
			publishTask: func(ch chan TaskReq) {
				for i := 0; i < 10; i++ {
					ch <- TaskReq{task: new(TestTask)}
				}
				close(ch)
			},
			expectContextError: nil,
		},
		{
			name: "execute task return error",
			taskMap: map[string]Task{
				"test_task": new(TestTask),
			},
			taskChan:        make(chan TaskReq),
			timeoutDuration: 100 * time.Millisecond,
			publishTask: func(ch chan TaskReq) {
				ch <- TaskReq{task: &TestTask{f: func() error { return errors.New("error") }}}
				close(ch)
			},
			expectContextError: nil,
		},
		{
			name: "context was timeout",
			taskMap: map[string]Task{
				"test_task": new(TestTask),
			},
			taskChan:           make(chan TaskReq),
			timeoutDuration:    5 * time.Millisecond,
			publishTask:        func(ch chan TaskReq) {},
			expectContextError: context.DeadlineExceeded,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(context.Background(), tt.timeoutDuration)
			defer cancel()

			w := &worker{
				taskMap:  tt.taskMap,
				taskChan: tt.taskChan,
			}

			go func() {
				tt.publishTask(tt.taskChan)
			}()

			w.Start(ctx)

			assert.ErrorIs(t, ctx.Err(), tt.expectContextError)
		})
	}
}
