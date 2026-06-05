package zfs

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecursiveDatasets_Append(t *testing.T) {
	paths := [...]string{"zroot", "zroot/foo", "zroot/bar", "zroot/baz"}

	tests := []struct {
		name     string
		paths    []string
		included []bool
		excluded []string
	}{
		{
			name:     "all included",
			paths:    paths[:],
			included: []bool{true, true, true, true},
		},
		{
			name:     "all excluded",
			paths:    paths[:],
			included: []bool{true, false, false, false},
			excluded: paths[1:],
		},
		{
			name:     "exclude 1",
			paths:    paths[:],
			included: []bool{true, false, true, true},
			excluded: []string{paths[1]},
		},
		{
			name:     "exclude 123",
			paths:    []string{paths[0], paths[1], "zroot/foo/bar", "zroot/foo/baz"},
			included: []bool{true, false, false, false},
			excluded: []string{paths[1]},
		},
		{
			name:     "exclude 2",
			paths:    paths[:],
			included: []bool{true, true, false, true},
			excluded: []string{paths[2]},
		},
		{
			name:     "exclude 3",
			paths:    paths[:],
			included: []bool{true, true, true, false},
			excluded: []string{paths[3]},
		},
	}

	root, err := NewDatasetPath(paths[0])
	require.NoError(t, err)
	require.NotNil(t, root)
	root.SetRecursive()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			roots := newRecursiveDatasets()
			datasets := make([]*DatasetPath, len(tt.paths))

			var wantExcluded []string
			for i, name := range tt.paths {
				p, err := NewDatasetPath(name)
				require.NoError(t, err)
				require.NotNil(t, p)
				datasets[i] = p
				roots.Add(root, p, tt.included[i])
				if !tt.included[i] {
					wantExcluded = append(wantExcluded, p.ToString())
				}
			}

			top := datasets[0]
			assert.Equal(t, tt.included[0], top.Recursive(),
				"top level %s has wrong Recursive()", top.ToString())
			assert.Nil(t, top.RecursiveParent(),
				"top level %s has RecursiveParent()", top.ToString())
			assert.Equal(t, len(tt.excluded) != 0, top.HasExcluded(),
				"top level %s has wrong HasExcluded()", top.ToString())

			var excluded []string
			for _, p := range datasets[1:] {
				assert.False(t, p.Recursive(), "wrong Recursive() from child %s",
					p.ToString())
				if top.Excluded(p) {
					assert.Nil(t, p.RecursiveParent(),
						"excluded %s has RecursiveParent()", p.ToString())
					excluded = append(excluded, p.ToString())
				} else {
					assert.Same(t, top, p.RecursiveParent(),
						"included %s has wrong RecursiveParent()", p.ToString())
				}
			}
			assert.Equal(t, wantExcluded, excluded, "wrong excluded datasets")

			if len(excluded) != 0 {
				excluded = excluded[:0]
			}

			for _, p := range top.exclude {
				excluded = append(excluded, p.ToString())
			}
			assert.Equal(t, tt.excluded, excluded)

			if len(excluded) != 0 {
				assert.Equal(t, strings.Join(excluded, ","), top.ExcludedString())
			} else {
				assert.Empty(t, top.ExcludedString())
			}
		})
	}
}
