package status

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHumanizeDuration(t *testing.T) {
	tests := []struct {
		name     string
		d        time.Duration
		expected string
	}{
		{
			name:     "0s",
			d:        0,
			expected: "0s",
		},
		{
			name:     "100ms",
			d:        100 * time.Millisecond,
			expected: "0s",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := humanizeDuration(tt.d)
			t.Log(s)
			assert.Equal(t, tt.expected, s)
		})
	}
}
