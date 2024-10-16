package semaphore

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dsh2dsh/zrepl/internal/daemon/logging/trace"
)

func TestSemaphore(t *testing.T) {
	const numGoroutines = 10
	const concurrentSemaphore = 6
	const sleepTime = 1 * time.Second

	begin := time.Now()

	sem := New(concurrentSemaphore)

	var acquisitions struct {
		beforeT, afterT uint32
	}

	rootCtx, endRoot := trace.WithTaskFromStack(context.Background())
	defer endRoot()
	_, add, waitEnd := trace.WithTaskGroup(rootCtx, "TestSemaphore")

	for i := 0; i < numGoroutines; i++ {
		// not capturing i so no need for local copy
		add(func(ctx context.Context) {
			res, err := sem.Acquire(ctx)
			require.NoError(t, err)
			defer res.Release()
			if time.Since(begin) > sleepTime {
				atomic.AddUint32(&acquisitions.afterT, 1)
			} else {
				atomic.AddUint32(&acquisitions.beforeT, 1)
			}
			time.Sleep(sleepTime)
		})
	}

	waitEnd()

	assert.Equal(t, uint32(concurrentSemaphore), acquisitions.beforeT)
	assert.Equal(t, uint32(numGoroutines-concurrentSemaphore), acquisitions.afterT)
}
