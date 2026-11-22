package dataset

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPath_AllPrefixStrings(t *testing.T) {
	const testPath = "tank/foo/bar/baz"

	tests := []struct {
		name   string
		path   string
		prefix string
		want   []string
	}{
		{
			name: "nil",
			path: testPath,
			want: []string{"tank", "tank/foo", "tank/foo/bar", "tank/foo/bar/baz"},
		},
		{
			name:   "tank",
			path:   testPath,
			prefix: "tank",
			want:   []string{"tank/foo", "tank/foo/bar", "tank/foo/bar/baz"},
		},
		{
			name:   "foo",
			path:   testPath,
			prefix: "tank/foo",
			want:   []string{"tank/foo/bar", "tank/foo/bar/baz"},
		},
		{
			name:   "bar",
			path:   testPath,
			prefix: "tank/foo/bar",
			want:   []string{"tank/foo/bar/baz"},
		},
		{
			name:   "baz",
			path:   testPath,
			prefix: testPath,
			want:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := NewPath(tt.path)
			require.NoError(t, err)
			require.NotNil(t, p)

			var prefix *Path
			if tt.prefix != "" {
				p, err := NewPath(tt.prefix)
				require.NoError(t, err)
				require.NotNil(t, p)
				prefix = p
			}
			assert.Equal(t, tt.want, slices.Collect(p.AllPrefixStrings(prefix)))
		})
	}
}
