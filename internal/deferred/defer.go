// Package deferred provides a simple way to defer a function call in a separate goroutine
// and allow multiple callers to wait for the result.
package deferred

import (
	"context"
	"sync"
	"time"
)

// RetryAfter is how long a failed result is cached before the next Get starts a fresh attempt.
const RetryAfter = 5 * time.Second

type attempt[T any] struct {
	done     chan struct{}
	value    T
	err      error
	finished time.Time
}

// Result represents a computation that runs in the background and is re-run on demand after a failure.
type Result[T any] struct {
	fn  func() (T, error)
	mu  sync.Mutex
	cur *attempt[T]
}

// Defer starts the provided function in a separate goroutine and returns a handle to the result.
func Defer[T any](deferFunc func() (T, error)) *Result[T] {
	r := &Result[T]{fn: deferFunc}
	r.cur = r.start()

	return r
}

func (d *Result[T]) start() *attempt[T] {
	a := &attempt[T]{done: make(chan struct{})}

	go func() {
		a.value, a.err = d.fn()
		a.finished = time.Now()
		close(a.done)
	}()

	return a
}

// Get blocks until the current attempt is finished or the context is canceled.
// A failed attempt older than RetryAfter is replaced by a new attempt (at most one in flight).
func (d *Result[T]) Get(ctx context.Context) (T, error) {
	d.mu.Lock()
	a := d.cur

	select {
	case <-a.done:
		if a.err != nil && time.Since(a.finished) >= RetryAfter {
			a = d.start()
			d.cur = a
		}
	default:
	}
	d.mu.Unlock()

	select {
	case <-ctx.Done():
		var zero T

		return zero, ctx.Err()
	case <-a.done:
		return a.value, a.err
	}
}
