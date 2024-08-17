package goworkers

import (
	"context"
	"runtime"
	"sync"
)

type Config struct {
	MaxThreads int
}

type Msg struct {
	TaskName string
	Params   interface{}
}

type WorkerPool struct {
	taskMap    map[string]Task
	maxThreads int
	msgChan    chan Msg
	taskChan   chan TaskReq
}

func NewWorkerPool(cfg Config) *WorkerPool {
	return &WorkerPool{
		taskMap:    make(map[string]Task),
		maxThreads: cfg.MaxThreads,
		msgChan:    make(chan Msg),
	}
}

func (w *WorkerPool) newWorker() *worker {
	return &worker{
		taskChan: w.taskChan,
	}
}

func (w *WorkerPool) Register(ctx context.Context, task Task) error {
	_, exist := w.taskMap[task.Name()]
	if exist {
		return ErrTaskAlreadyExist
	}

	w.taskMap[task.Name()] = task

	return nil
}

func (w *WorkerPool) Start(ctx context.Context) error {
	// read from stream for each tasks
	for _, task := range w.taskMap {
		task := task

		go func() {
			task.Subscribe(ctx, w.msgChan)
		}()
	}

	// parse item in msgChan to taskChan
	w.taskChan = make(chan TaskReq)
	go func() {
		for {
			select {
			case msg := <-w.msgChan:
				task, exist := w.taskMap[msg.TaskName]
				if !exist {
					continue
				}

				w.taskChan <- TaskReq{
					task:   task,
					params: msg.Params,
				}
			case <-ctx.Done():
				return
			}
			runtime.GC()
		}
	}()

	var wg sync.WaitGroup

	// start workers
	// TODO: close workers if they idle for too long
	// TODO: re-create workers if they were closed and there are more request comes in
	for i := 0; i < w.maxThreads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			w.newWorker().Start(ctx)
		}()
	}

	wg.Wait()

	return ctx.Err()
}

func (w *WorkerPool) Stop() error {
	for _, task := range w.taskMap {
		task.Unsubscribe()
	}

	close(w.msgChan)
	close(w.taskChan)

	return nil
}
