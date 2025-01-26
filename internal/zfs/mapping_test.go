package zfs

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_recursiveDatasets(t *testing.T) {
	tests := []struct {
		name      string
		included  []bool
		recursive bool
	}{
		{
			name:      "all included",
			included:  []bool{true, true, true},
			recursive: true,
		},
		{
			name:     "all excluded",
			included: []bool{false, false, false},
		},
		{
			name:     "exclude 1",
			included: []bool{false, true, true},
		},
		{
			name:     "exclude 2",
			included: []bool{true, false, true},
		},
		{
			name:     "exclude 3",
			included: []bool{true, true, false},
		},
	}

	root, err := NewDatasetPath("zroot")
	require.NoError(t, err)
	require.NotNil(t, root)

	paths := []string{"zroot/foo", "zroot/bar", "zroot/baz"}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			roots := newRecursiveDatasets()
			datasets := make([]*DatasetPath, len(paths))

			for i, name := range paths {
				p, err := NewDatasetPath(name)
				require.NoError(t, err)
				require.NotNil(t, p)
				datasets[i] = p
				roots.Add(root, p, tt.included[i])
			}
			roots.UpdateChildren()

			if tt.recursive {
				assert.Equal(t, -1, slices.IndexFunc(datasets,
					func(p *DatasetPath) bool {
						return p.RecursiveParent() != root
					}))
			} else {
				assert.Equal(t, -1, slices.IndexFunc(datasets,
					func(p *DatasetPath) bool {
						return p.RecursiveParent() != nil
					}))
			}
		})
	}
}
