package nanosleep

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSleepUntil(t *testing.T) {
	startTime := time.Now()
	endTime := startTime.Add(time.Second)
	require.NoError(t, SleepUntil(endTime))

	d := time.Since(startTime)
	assert.GreaterOrEqual(t, d, time.Second)
	assert.Less(t, d, 2*time.Second)
}

func TestNewTimer(t *testing.T) {
	startTime := time.Now()
	timer := NewTimer(time.Second)
	endTime := <-timer.C()

	d := time.Since(startTime)
	assert.GreaterOrEqual(t, d, time.Second)
	assert.Less(t, d, 2*time.Second)
	assert.GreaterOrEqual(t, endTime.Sub(startTime), time.Second)
	assert.Less(t, endTime.Sub(startTime), 2*time.Second)
}

func TestTimer_Reset(t *testing.T) {
	startTime := time.Now()
	timer := NewTimer(time.Second)

	time.Sleep(500 * time.Millisecond)
	timer.Reset(time.Second)
	endTime := <-timer.C()

	d := time.Since(startTime)
	assert.GreaterOrEqual(t, d, time.Second)
	assert.Less(t, d, 2*time.Second)
	assert.GreaterOrEqual(t, endTime.Sub(startTime), time.Second)
	assert.Less(t, endTime.Sub(startTime), 2*time.Second)
}
