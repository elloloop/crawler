// Package redis provides a Redis Streams-backed Queue implementation.
package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"

	crawlerv1 "github.com/elloloop/crawler/gen/go/crawler/v1"
	"github.com/elloloop/crawler/internal/queue"
)

type redisQueue struct {
	client   *redis.Client
	stream   string
	group    string
	consumer string
}

// New creates a Redis Streams-backed queue. It creates the consumer group on
// startup, ignoring BUSYGROUP errors (meaning the group already exists).
func New(addr, password, streamKey, group, consumer string) (queue.Queue, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	// Create the consumer group. Use "0" to read from the beginning.
	// Ignore BUSYGROUP errors which indicate the group already exists.
	err := client.XGroupCreateMkStream(ctx, streamKey, group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		client.Close()
		return nil, fmt.Errorf("create consumer group: %w", err)
	}

	return &redisQueue{
		client:   client,
		stream:   streamKey,
		group:    group,
		consumer: consumer,
	}, nil
}

func (q *redisQueue) Push(ctx context.Context, event *crawlerv1.URLFrontierEvent) error {
	data, err := protojson.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	return q.client.XAdd(ctx, &redis.XAddArgs{
		Stream: q.stream,
		Values: map[string]interface{}{
			"data": string(data),
		},
	}).Err()
}

func (q *redisQueue) Pop(ctx context.Context) (*crawlerv1.URLFrontierEvent, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		streams, err := q.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    q.group,
			Consumer: q.consumer,
			Streams:  []string{q.stream, ">"},
			Count:    1,
			Block:    2 * time.Second,
		}).Result()
		if err != nil {
			if err == redis.Nil {
				continue
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("xreadgroup: %w", err)
		}

		for _, stream := range streams {
			for _, msg := range stream.Messages {
				data, ok := msg.Values["data"].(string)
				if !ok {
					// ACK malformed message and skip
					q.client.XAck(ctx, q.stream, q.group, msg.ID)
					continue
				}

				event := &crawlerv1.URLFrontierEvent{}
				if err := protojson.Unmarshal([]byte(data), event); err != nil {
					q.client.XAck(ctx, q.stream, q.group, msg.ID)
					continue
				}

				if err := q.client.XAck(ctx, q.stream, q.group, msg.ID).Err(); err != nil {
					return nil, fmt.Errorf("xack: %w", err)
				}

				return event, nil
			}
		}
	}
}

func (q *redisQueue) Len(ctx context.Context) (int64, error) {
	return q.client.XLen(ctx, q.stream).Result()
}

func (q *redisQueue) Close() error {
	return q.client.Close()
}
