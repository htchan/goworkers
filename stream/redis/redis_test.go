package redis

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/redis/rueidis"
	"github.com/redis/rueidis/rueidiscompat"
	"github.com/stretchr/testify/assert"
)

var (
	cli rueidis.Client
)

func TestMain(m *testing.M) {
	// leak := flag.Bool("leak", false, "check for memory leaks")
	flag.Parse()

	redisAddr, purge, err := setupContainer()
	if err != nil {
		purge()
		log.Fatalf("fail to setup docker: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	c, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{redisAddr},
	})
	if err != nil {
		purge()
		log.Fatalf("fail to create redis client: %v", err)
	}
	cli = c
	defer cli.Close()

	code := m.Run()

	purge()
	os.Exit(code)
}

func setupContainer() (string, func(), error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		return "", func() {}, fmt.Errorf("init docker fail: %w", err)
	}

	containerName := "goworker_test_redis"
	pool.RemoveContainerByName(containerName)

	resource, err := pool.RunWithOptions(
		&dockertest.RunOptions{
			Repository: "redis",
			Tag:        "7",
			Name:       containerName,
		},
		func(hc *docker.HostConfig) {
			hc.AutoRemove = true
		},
	)
	if err != nil {
		return "", func() {}, fmt.Errorf("create resource fail: %w", err)
	}

	purge := func() {
		if resource.Close() != nil {
			fmt.Println("purge error", err)
		}
	}

	return resource.GetHostPort("6379/tcp"), purge, nil
}

func TestNewRedisStream(t *testing.T) {
	t.Parallel()

	type params struct {
		cli                             rueidis.Client
		stream, groupName, consumerName string
		cfg                             Config
		idleTime                        time.Duration
	}

	tests := []struct {
		name   string
		params params
		want   *RedisStream
	}{
		{
			name: "happy flow",
			params: params{
				cli:          nil,
				stream:       "test",
				groupName:    "testGroup",
				consumerName: "testConsumer",
				cfg: Config{
					IdleDuration:  time.Second,
					BlockDuration: 100 * time.Millisecond,
				},
			},
			want: &RedisStream{
				cli:          rueidiscompat.NewAdapter(nil),
				stream:       "test",
				groupName:    "testGroup",
				consumerName: "testConsumer",
				cfg: Config{
					IdleDuration:  time.Second,
					BlockDuration: 100 * time.Millisecond,
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			get := NewRedisStream(tt.params.cli, tt.params.stream, tt.params.groupName, tt.params.consumerName, tt.params.cfg)
			assert.Equal(t, tt.want, get)
		})
	}
}

func TestRedisStream_CreateStream(t *testing.T) {
	t.Parallel()

	existingStream := &RedisStream{
		cli:          rueidiscompat.NewAdapter(cli),
		stream:       "existing_stream",
		groupName:    "existing_group",
		consumerName: "existing_consumer",
	}
	existingStream.CreateStream(context.Background())

	tests := []struct {
		name    string
		stream  *RedisStream
		getCtx  func() context.Context
		wantErr error
	}{
		{
			name: "happy flow",
			stream: &RedisStream{
				cli:          rueidiscompat.NewAdapter(cli),
				stream:       "new_stream",
				groupName:    "new_group",
				consumerName: "new_consumer",
			},
			getCtx: func() context.Context {
				return context.Background()
			},
			wantErr: nil,
		},
		{
			name: "context deadline exceed",
			stream: &RedisStream{
				cli:          rueidiscompat.NewAdapter(cli),
				stream:       "deadline_stream",
				groupName:    "deadline_group",
				consumerName: "deadline_consumer",
			},
			getCtx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				return ctx
			},
			wantErr: context.Canceled,
		},
		{
			name:   "create repeated streams",
			stream: existingStream,
			getCtx: func() context.Context {
				return context.Background()
			},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.stream.CreateStream(tt.getCtx())
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestRedisStream_Publish(t *testing.T) {
	t.Parallel()

	stream := &RedisStream{
		cli:          rueidiscompat.NewAdapter(cli),
		stream:       "test_publish",
		groupName:    "testGroup",
		consumerName: "testConsumer",
	}

	tests := []struct {
		name    string
		stream  *RedisStream
		getCtx  func() context.Context
		msg     interface{}
		wantErr error
	}{
		{
			name:   "happy flow",
			stream: stream,
			getCtx: func() context.Context {
				return context.Background()
			},
			msg:     map[string]interface{}{"key": "valie"},
			wantErr: nil,
		},
		{
			name:   "context deadline exceed",
			stream: stream,
			getCtx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				return ctx
			},
			msg:     "test",
			wantErr: context.Canceled,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.stream.Publish(tt.getCtx(), tt.msg)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestRedisStream_Subscribe(t *testing.T) {
}

func TestRedisStream_readPendingMsg(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		getCtx  func() context.Context
		stream  *RedisStream
		wants   []interface{}
		wantErr error
	}{
		{
			name: "happy flow",
			getCtx: func() context.Context {
				return context.Background()
			},
			stream: &RedisStream{
				cli:          rueidiscompat.NewAdapter(cli),
				stream:       "test_read_pending__happy_flow",
				groupName:    "testGroup",
				consumerName: "testConsumer",
				cfg: Config{
					IdleDuration:  time.Millisecond,
					BlockDuration: 100 * time.Millisecond,
				},
			},
			wants:   []interface{}{map[string]interface{}{"key": "value2"}},
			wantErr: nil,
		},
		{
			name: "context cancelled",
			getCtx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				return ctx
			},
			stream: &RedisStream{
				cli:          rueidiscompat.NewAdapter(cli),
				stream:       "test_read_pending__context_cancelled",
				groupName:    "testGroup",
				consumerName: "testConsumer", cfg: Config{
					IdleDuration:  time.Millisecond,
					BlockDuration: 100 * time.Millisecond,
				},
			},
			wants:   nil,
			wantErr: context.Canceled,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := tt.getCtx()

			// create pending msgs and ack the first one
			tt.stream.cli.XAdd(ctx, rueidiscompat.XAddArgs{
				Stream: tt.stream.stream,
				Values: map[string]interface{}{"key": "value"},
			})
			tt.stream.cli.XAdd(ctx, rueidiscompat.XAddArgs{
				Stream: tt.stream.stream,
				Values: map[string]interface{}{"key": "value2"},
			})
			tt.stream.cli.XGroupCreateMkStream(ctx, tt.stream.stream, tt.stream.groupName, "0-0")
			result, _ := tt.stream.cli.XReadGroup(ctx, rueidiscompat.XReadGroupArgs{
				Group:    tt.stream.groupName,
				Consumer: tt.stream.consumerName,
				Streams:  []string{tt.stream.stream, ">"},
				Count:    10,
				Block:    100 * time.Millisecond,
				NoAck:    false,
			}).Result()
			if len(result) > 0 {
				tt.stream.cli.XAck(ctx, tt.stream.stream, tt.stream.groupName, result[0].Messages[0].ID)
			}
			time.Sleep(10 * time.Millisecond)

			// read pending msg
			var wg sync.WaitGroup
			ch := make(chan interface{})
			wg.Add(1)
			go func() {
				assert.ErrorIs(t, tt.stream.readPendingMsg(ctx, ch), tt.wantErr)
				time.Sleep(10 * time.Millisecond)
				close(ch)
				wg.Done()
			}()

			// validate pending msg
			var gots []interface{}
			for msg := range ch {
				got, ok := msg.(rueidiscompat.XMessage)
				assert.True(t, ok, "parse msg to rueidiscompat.XMessage")
				gots = append(gots, got.Values)
			}
			assert.Equal(t, tt.wants, gots)

			wg.Wait()
		})
	}
}

func TestRedisStream_readMsg(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		getCtx  func() context.Context
		stream  *RedisStream
		wants   []interface{}
		wantErr error
	}{
		{
			name: "happy flow",
			getCtx: func() context.Context {
				return context.Background()
			},
			stream: &RedisStream{
				cli:          rueidiscompat.NewAdapter(cli),
				stream:       "test_read__happy_flow",
				groupName:    "testGroup",
				consumerName: "testConsumer",
				cfg: Config{
					IdleDuration:  time.Millisecond,
					BlockDuration: 100 * time.Millisecond,
				},
			},
			wants:   []interface{}{map[string]interface{}{"key": "value2"}},
			wantErr: nil,
		},
		{
			name: "context cancelled",
			getCtx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				return ctx
			},
			stream: &RedisStream{
				cli:          rueidiscompat.NewAdapter(cli),
				stream:       "test_read__context_cancelled",
				groupName:    "testGroup",
				consumerName: "testConsumer",
				cfg: Config{
					IdleDuration:  time.Millisecond,
					BlockDuration: 100 * time.Millisecond,
				},
			},
			wants:   nil,
			wantErr: context.Canceled,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := tt.getCtx()

			// publish msgs and consumer the first one
			tt.stream.cli.XAdd(ctx, rueidiscompat.XAddArgs{
				Stream: tt.stream.stream,
				Values: map[string]interface{}{"key": "value"},
			})
			tt.stream.cli.XGroupCreateMkStream(ctx, tt.stream.stream, tt.stream.groupName, "0-0")
			tt.stream.cli.XReadGroup(ctx, rueidiscompat.XReadGroupArgs{
				Group:    tt.stream.groupName,
				Consumer: tt.stream.consumerName,
				Streams:  []string{tt.stream.stream, ">"},
				Count:    10,
				Block:    100 * time.Millisecond,
				NoAck:    false,
			})
			tt.stream.cli.XAdd(ctx, rueidiscompat.XAddArgs{
				Stream: tt.stream.stream,
				Values: map[string]interface{}{"key": "value2"},
			})

			var wg sync.WaitGroup
			ch := make(chan interface{})
			wg.Add(1)
			go func() {
				assert.ErrorIs(t, tt.stream.readMsg(ctx, ch), tt.wantErr)
				time.Sleep(10 * time.Millisecond)
				close(ch)
				wg.Done()
			}()

			// validate subscribed msg
			var gots []interface{}
			for msg := range ch {
				got, ok := msg.(rueidiscompat.XMessage)
				assert.True(t, ok, "parse msg to rueidiscompat.XMessage")
				gots = append(gots, got.Values)
			}
			assert.Equal(t, tt.wants, gots)

			wg.Wait()
		})
	}
}

func TestRedisStream_Acknowledge(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		getCtx  func() context.Context
		stream  *RedisStream
		wantErr error
	}{
		{
			name: "happy flow",
			getCtx: func() context.Context {
				return context.Background()
			},
			stream: &RedisStream{
				cli:          rueidiscompat.NewAdapter(cli),
				stream:       "test_acknowledge__happy_flow",
				groupName:    "testGroup",
				consumerName: "testConsumer",
				cfg: Config{
					IdleDuration:  time.Millisecond,
					BlockDuration: 100 * time.Millisecond,
				},
			},
			wantErr: nil,
		},
		{
			name: "context cancelled",
			getCtx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				return ctx
			},
			stream: &RedisStream{
				cli:          rueidiscompat.NewAdapter(cli),
				stream:       "test_acknowledge__context_cancelled",
				groupName:    "testGroup",
				consumerName: "testConsumer",
				cfg: Config{
					IdleDuration:  time.Millisecond,
					BlockDuration: 100 * time.Millisecond,
				},
			},
			wantErr: context.Canceled,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := tt.getCtx()

			// publish msg
			tt.stream.cli.XAdd(ctx, rueidiscompat.XAddArgs{
				Stream: tt.stream.stream,
				Values: map[string]interface{}{"key": "value"},
			})
			tt.stream.cli.XGroupCreateMkStream(ctx, tt.stream.stream, tt.stream.groupName, "0-0")
			result, err := tt.stream.cli.XReadGroup(ctx, rueidiscompat.XReadGroupArgs{
				Group:    tt.stream.groupName,
				Consumer: tt.stream.consumerName,
				Streams:  []string{tt.stream.stream, ">"},
				Count:    10,
				Block:    100 * time.Millisecond,
				NoAck:    false,
			}).Result()
			if err == nil {
				err := tt.stream.Acknowledge(ctx, result[0].Messages[0])
				assert.ErrorIs(t, err, tt.wantErr)
			}
			time.Sleep(10 * time.Millisecond)
			pendingResult, _ := tt.stream.cli.XPending(ctx, tt.stream.stream, tt.stream.groupName).Result()
			assert.Equal(t, int64(0), pendingResult.Count)
		})
	}
}
