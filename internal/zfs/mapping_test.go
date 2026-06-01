package zfs

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_recursiveDatasets(t *testing.T) {
	paths := []string{"zroot", "zroot/foo", "zroot/bar", "zroot/baz"}

	tests := []struct {
		name      string
		included  []bool
		recursive bool
	}{
		{
			name:      "all included",
			included:  []bool{true, true, true, true},
			recursive: true,
		},
		{
			name:     "all excluded",
			included: []bool{false, false, false, false},
		},
		{
			name:     "exclude 1",
			included: []bool{true, false, true, true},
		},
		{
			name:     "exclude 2",
			included: []bool{true, true, false, true},
		},
		{
			name:     "exclude 3",
			included: []bool{true, true, true, false},
		},
	}

	root, err := NewDatasetPath("zroot")
	require.NoError(t, err)
	require.NotNil(t, root)

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

			assert.Equal(t, tt.recursive, datasets[0].Recursive())

			var countRecursive int
			for _, p := range datasets {
				if p.Recursive() {
					countRecursive++
				}
			}

			if tt.recursive {
				assert.Equal(t, 1, countRecursive)
				assert.Equal(t, -1, slices.IndexFunc(datasets,
					func(p *DatasetPath) bool {
						return p.RecursiveParent() != root
					}))
			} else {
				assert.Zero(t, countRecursive)
				assert.Equal(t, -1, slices.IndexFunc(datasets,
					func(p *DatasetPath) bool {
						return p.RecursiveParent() != nil
					}))
			}
		})
	}
}
