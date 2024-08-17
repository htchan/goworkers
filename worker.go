package goworkers

import (
	"context"
	"log/slog"
)

type TaskReq struct {
	task   Task
	params interface{}
}

type worker struct {
	taskChan chan TaskReq
}

func (w *worker) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case taskReq, ok := <-w.taskChan:
			if !ok {
				return ctx.Err()
			}

			err := taskReq.task.Execute(ctx, taskReq.params)
			if err != nil {
				slog.Error(
					"task failed",
					slog.String("task", taskReq.task.Name()),
					slog.String("err", err.Error()),
				)
			} else {
				slog.Debug("task completed", slog.String("task", taskReq.task.Name()))
			}
		}
	}
}
