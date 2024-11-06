package semaphore

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	ctx := context.Background()

	var wg sync.WaitGroup
	add := func(f func(context.Context)) {
		wg.Add(1)
		go func() {
			f(ctx)
			wg.Done()
		}()
	}
	waitEnd := func() { wg.Wait() }

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
