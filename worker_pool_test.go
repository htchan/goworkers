package goworkers

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewWorkerPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  Config
		want *WorkerPool
	}{
		{
			name: "happy flow",
			cfg:  Config{MaxThreads: 10},
			want: &WorkerPool{
				maxThreads: 10,
				taskMap:    map[string]Task{},
				msgChan:    make(chan Msg),
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := NewWorkerPool(tt.cfg)
			assert.Equal(t, tt.want.taskMap, got.taskMap)
			assert.Equal(t, len(tt.want.msgChan), len(got.msgChan))
			assert.Equal(t, len(tt.want.taskChan), len(got.taskChan))
		})
	}
}

func TestWorkerPool_newWorker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		pool *WorkerPool
		want *worker
	}{
		{
			name: "happy flow",
			pool: &WorkerPool{maxThreads: 10},
			want: &worker{},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.pool.newWorker()
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestWorkerPool_Register(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pool     *WorkerPool
		tasks    []Task
		wantPool *WorkerPool
		wantErr  error
	}{
		{
			name:     "happy flow",
			pool:     &WorkerPool{maxThreads: 10, taskMap: make(map[string]Task)},
			tasks:    []Task{&TestTask{}},
			wantPool: &WorkerPool{maxThreads: 10, taskMap: map[string]Task{"test_task": &TestTask{}}},
		},
		{
			name:     "task already exist",
			pool:     &WorkerPool{maxThreads: 10, taskMap: map[string]Task{"test_task": &TestTask{}}},
			tasks:    []Task{&TestTask{}},
			wantPool: &WorkerPool{maxThreads: 10, taskMap: map[string]Task{"test_task": &TestTask{}}},
			wantErr:  ErrTaskAlreadyExist,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()

			err := tt.pool.Register(ctx, tt.tasks...)
			assert.Equal(t, tt.wantPool, tt.pool)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestWorkerPool_Start(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		getCtx   func(*testing.T) context.Context
		pool     *WorkerPool
		wantPool *WorkerPool
		wantErr  error
	}{
		{
			name: "happy flow",
			getCtx: func(t *testing.T) context.Context {
				return context.Background()
			},
			pool: &WorkerPool{
				maxThreads: 10,
				taskMap:    map[string]Task{"test_task": &TestTask{}},
				msgChan:    make(chan Msg),
			},
			wantPool: &WorkerPool{
				maxThreads: 10,
				taskMap:    map[string]Task{"test_task": &TestTask{}},
				msgChan:    make(chan Msg),
				taskChan:   make(chan TaskReq),
			},
		},
		{
			name: "context already end",
			getCtx: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				return ctx
			},
			pool: &WorkerPool{
				maxThreads: 10,
				taskMap:    map[string]Task{"test_task": &TestTask{}},
				msgChan:    make(chan Msg),
			},
			wantPool: &WorkerPool{
				maxThreads: 10,
				taskMap:    map[string]Task{"test_task": &TestTask{}},
				msgChan:    make(chan Msg),
				taskChan:   make(chan TaskReq),
			},
			wantErr: context.Canceled,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var wg sync.WaitGroup
			wg.Add(1)

			go func() {
				err := tt.pool.Start(tt.getCtx(t))
				assert.Equal(t, tt.wantPool.taskMap, tt.pool.taskMap)
				assert.Equal(t, len(tt.wantPool.msgChan), len(tt.pool.msgChan))
				assert.Equal(t, len(tt.wantPool.taskChan), len(tt.pool.taskChan))
				assert.ErrorIs(t, err, tt.wantErr)
				wg.Done()
			}()

			time.Sleep(time.Millisecond)
			tt.pool.Stop()

			wg.Wait()
		})
	}
}
