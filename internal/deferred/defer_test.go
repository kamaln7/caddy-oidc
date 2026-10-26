package deferred

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResult_SuccessIsCached(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32

	r := Defer(func() (int, error) {
		calls.Add(1)

		return 42, nil
	})

	for range 3 {
		v, err := r.Get(context.Background())
		require.NoError(t, err)
		assert.Equal(t, 42, v)
	}

	assert.EqualValues(t, 1, calls.Load())
}

func TestResult_FailureIsCachedThenRetried(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32

	r := Defer(func() (int, error) {
		if calls.Add(1) == 1 {
			return 0, errors.New("idp down")
		}

		return 7, nil
	})

	_, err := r.Get(context.Background())
	require.Error(t, err)

	// Within RetryAfter the failure is served from cache without a new attempt.
	_, err = r.Get(context.Background())
	require.Error(t, err)
	assert.EqualValues(t, 1, calls.Load())

	// Age the failed attempt past RetryAfter; the next Get starts a fresh attempt that succeeds.
	r.mu.Lock()
	r.cur.finished = time.Now().Add(-2 * RetryAfter)
	r.mu.Unlock()

	v, err := r.Get(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 7, v)
	assert.EqualValues(t, 2, calls.Load())
}

func TestResult_GetHonoursContext(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	defer close(release)

	r := Defer(func() (int, error) {
		<-release

		return 0, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := r.Get(ctx)
	require.ErrorIs(t, err, context.Canceled)
}
