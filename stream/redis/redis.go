package redis

import (
	"context"
	"errors"
	"time"

	"github.com/htchan/goworkers/stream"
	"github.com/redis/rueidis"
	"github.com/redis/rueidis/rueidiscompat"
	"golang.org/x/exp/slog"
)

type RedisStream struct {
	cli          rueidiscompat.Cmdable
	stream       string
	groupName    string
	consumerName string
	idleTime     time.Duration
}

var _ stream.Stream = (*RedisStream)(nil)

func NewRedisStream(cli rueidis.Client, stream, groupName, consumerName string, idleTime time.Duration) *RedisStream {
	return &RedisStream{
		cli:          rueidiscompat.NewAdapter(cli),
		stream:       stream,
		groupName:    groupName,
		consumerName: consumerName,
		idleTime:     idleTime,
	}
}

func (s *RedisStream) Publish(ctx context.Context, msg interface{}) error {
	_, err := s.cli.XAdd(ctx, rueidiscompat.XAddArgs{
		Stream: s.stream,
		Values: msg,
	}).Result()

	return err
}

func (s *RedisStream) Subscribe(ctx context.Context, ch chan interface{}) error {
	for {
		// read pending msg
		pendingMsgErr := s.readPendingMsg(ctx, ch)
		if pendingMsgErr != nil {
			return pendingMsgErr
		}

		//TODO: regular trim old msg in streams

		// read new msg
		msgErr := s.readMsg(ctx, ch)
		if msgErr != nil {
			return msgErr
		}
	}
}

func (s *RedisStream) readPendingMsg(ctx context.Context, ch chan interface{}) error {
	pendingMsgs, _, err := s.cli.XAutoClaim(ctx, rueidiscompat.XAutoClaimArgs{
		Stream:   s.stream,
		Group:    s.groupName,
		Start:    "0-0",
		Consumer: s.consumerName,
		MinIdle:  s.idleTime,
		// Count:    100,
	}).Result()
	if err != nil {
		if err == ctx.Err() {
			return err
		} else if errors.Is(err, rueidis.Nil) {
			return nil
		} else {
			slog.Error("readPendingMsg", slog.String("err", err.Error()), slog.String("stream", s.stream), slog.String("group", s.groupName), slog.String("consumer", s.consumerName))
		}
	}

	for _, msg := range pendingMsgs {
		slog.Debug("readPendingMsg", slog.String("stream", s.stream), slog.String("group", s.groupName), slog.String("consumer", s.consumerName), slog.String("msg_id", msg.ID), slog.Any("values", msg.Values))
		ch <- msg
	}

	return nil
}

func (s *RedisStream) readMsg(ctx context.Context, ch chan interface{}) error {
	// read new msg from stream
	streams, err := s.cli.XReadGroup(ctx, rueidiscompat.XReadGroupArgs{
		Group:    s.groupName,
		Consumer: s.consumerName,
		Streams:  []string{s.stream, ">"},
		Count:    100,
		Block:    100 * time.Millisecond,
		NoAck:    false,
	}).Result()
	if err != nil {
		if err == ctx.Err() {
			return err
		} else if errors.Is(err, rueidis.Nil) {
			return nil
		} else {
			slog.Error("readMsg", slog.String("err", err.Error()), slog.String("stream", s.stream), slog.String("group", s.groupName), slog.String("consumer", s.consumerName))
		}
	}

	for _, stream := range streams {
		for _, msg := range stream.Messages {
			slog.Debug("readMsg", slog.String("stream", s.stream), slog.String("group", s.groupName), slog.String("consumer", s.consumerName), slog.String("msg_id", msg.ID), slog.Any("values", msg.Values))
			ch <- msg
		}
	}

	return nil
}

func (s *RedisStream) Acknowledge(ctx context.Context, msg interface{}) error {
	parseMsg, ok := msg.(rueidiscompat.XMessage)
	if !ok {
		return errors.New("invalid msg")
	}

	return s.cli.XAck(ctx, s.stream, s.groupName, parseMsg.ID).Err()
}
