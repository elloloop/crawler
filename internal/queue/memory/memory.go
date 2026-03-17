// Package memory provides an in-process channel-based Queue implementation.
package memory

import (
	"context"
	"sync/atomic"

	crawlerv1 "github.com/elloloop/crawler/gen/go/crawler/v1"
	"github.com/elloloop/crawler/internal/queue"
)

const defaultCapacity = 100_000

type memQueue struct {
	ch   chan *crawlerv1.URLFrontierEvent
	size atomic.Int64
}

// New creates a channel-backed queue with the given capacity.
// If capacity is 0, a default of 100,000 is used.
func New(capacity int) queue.Queue {
	if capacity <= 0 {
		capacity = defaultCapacity
	}
	return &memQueue{
		ch: make(chan *crawlerv1.URLFrontierEvent, capacity),
	}
}

func (q *memQueue) Push(_ context.Context, event *crawlerv1.URLFrontierEvent) error {
	q.ch <- event
	q.size.Add(1)
	return nil
}

func (q *memQueue) Pop(ctx context.Context) (*crawlerv1.URLFrontierEvent, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case event := <-q.ch:
		q.size.Add(-1)
		return event, nil
	}
}

func (q *memQueue) Len(_ context.Context) (int64, error) {
	return q.size.Load(), nil
}

func (q *memQueue) Close() error {
	close(q.ch)
	return nil
}
