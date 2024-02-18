package goworkers

import "context"

type TaskReq struct {
	task   Task
	params interface{}
}

type worker struct {
	taskMap  map[string]Task
	taskChan chan TaskReq
}

func (w *worker) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case taskReq := <-w.taskChan:
			taskReq.task.Execute(ctx, taskReq.params)
		}
	}
}
