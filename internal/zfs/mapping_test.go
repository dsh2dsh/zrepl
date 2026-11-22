package zfs

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/zfs/dataset"
	"github.com/dsh2dsh/zrepl/internal/zfs/filter"
)

func TestRecursiveDatasets_Append(t *testing.T) {
	paths := [...]string{"zroot", "zroot/foo", "zroot/bar", "zroot/baz"}

	tests := []struct {
		name        string
		paths       []string
		included    []bool
		excluded    []string
		selfExclude bool
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
		{
			name: "self-exclude",
			paths: []string{
				"zroot",
				"zroot/data",
				"zroot/data/1",
				"zroot/data/1/2",
				"zroot/data/1/2/3",
			},
			included:    []bool{true, false, true, true, true},
			excluded:    []string{"zroot/data"},
			selfExclude: true,
		},
	}

	root, err := dataset.NewPath(paths[0])
	require.NoError(t, err)
	require.NotNil(t, root)
	root.WithRecursive(true)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			roots := newRecursiveDatasets()
			datasets := make([]*dataset.Path, len(tt.paths))

			var wantExcluded []string
			for i, name := range tt.paths {
				p, err := dataset.NewPath(name)
				require.NoError(t, err)
				require.NotNil(t, p)
				datasets[i] = p
				roots.Add(root, p, tt.included[i])
				if !tt.included[i] {
					p.WithRecursive(!tt.selfExclude)
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
			for i, p := range datasets[1:] {
				wantRecursive := !tt.included[i+1] && !tt.selfExclude
				assert.Equal(t, wantRecursive, p.Recursive(),
					"wrong Recursive() from child %s", p.ToString())

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

			for _, p := range top.ExcludedPaths() {
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

func Test_filterDatasets(t *testing.T) {
	filterRecursive := true
	filterNotRecursive := false

	tests := []struct {
		name      string
		filter    []config.DatasetFilter
		all       []string
		expect    []string
		recursive map[string]bool
		parents   map[string]string
		excluded  map[string]string
	}{
		{
			name: "zroot recursive",
			filter: []config.DatasetFilter{
				{Pattern: "zroot", Recursive: &filterRecursive},
			},
			all:       []string{"zroot", "zroot/foo", "zroot/bar", "zroot/baz"},
			expect:    []string{"zroot", "zroot/foo", "zroot/bar", "zroot/baz"},
			recursive: map[string]bool{"zroot": true},
			parents: map[string]string{
				"zroot/foo": "zroot",
				"zroot/bar": "zroot",
				"zroot/baz": "zroot",
			},
			excluded: map[string]string{},
		},
		{
			name:      "zroot included",
			filter:    []config.DatasetFilter{{Pattern: "zroot"}},
			all:       []string{"zroot", "zroot/foo", "zroot/bar", "zroot/baz"},
			expect:    []string{"zroot"},
			recursive: map[string]bool{},
			parents:   map[string]string{},
			excluded:  map[string]string{},
		},
		{
			name: "foo excluded",
			filter: []config.DatasetFilter{
				{Pattern: "zroot", Recursive: &filterRecursive},
				{Pattern: "zroot/foo", Exclude: true},
			},
			all:       []string{"zroot", "zroot/foo", "zroot/bar", "zroot/baz"},
			expect:    []string{"zroot", "zroot/bar", "zroot/baz"},
			recursive: map[string]bool{"zroot": true},
			parents: map[string]string{
				"zroot/bar": "zroot",
				"zroot/baz": "zroot",
			},
			excluded: map[string]string{"zroot": "zroot/foo"},
		},
		{
			name: "foo excluded recursive",
			filter: []config.DatasetFilter{
				{Pattern: "zroot", Recursive: &filterRecursive},
				{Pattern: "zroot/foo", Exclude: true},
			},
			all: []string{
				"zroot",
				"zroot/foo",
				"zroot/foo/bar",
				"zroot/foo/baz",
				"zroot/bar",
			},
			expect:    []string{"zroot", "zroot/bar"},
			recursive: map[string]bool{"zroot": true},
			parents:   map[string]string{"zroot/bar": "zroot"},
			excluded:  map[string]string{"zroot": "zroot/foo"},
		},
		{
			name: "foo self-excluded",
			filter: []config.DatasetFilter{
				{Pattern: "zroot", Recursive: &filterRecursive},
				{Pattern: "zroot/foo", Exclude: true, Recursive: &filterNotRecursive},
			},
			all: []string{
				"zroot",
				"zroot/foo",
				"zroot/foo/bar",
				"zroot/foo/baz",
				"zroot/bar",
			},
			expect: []string{
				"zroot",
				"zroot/foo/bar",
				"zroot/foo/baz",
				"zroot/bar",
			},
			recursive: map[string]bool{"zroot": true},
			parents: map[string]string{
				"zroot/foo/bar": "zroot",
				"zroot/foo/baz": "zroot",
				"zroot/bar":     "zroot",
			},
			excluded: map[string]string{"zroot": "zroot/foo"},
		},
		{
			name: "baz excluded",
			filter: []config.DatasetFilter{
				{Pattern: "zroot", Recursive: &filterRecursive},
				{Pattern: "zroot/baz", Exclude: true},
			},
			all:       []string{"zroot", "zroot/foo", "zroot/bar", "zroot/baz"},
			expect:    []string{"zroot", "zroot/foo", "zroot/bar"},
			recursive: map[string]bool{"zroot": true},
			parents: map[string]string{
				"zroot/foo": "zroot",
				"zroot/bar": "zroot",
			},
			excluded: map[string]string{"zroot": "zroot/baz"},
		},
		{
			name: "self-excluded",
			filter: []config.DatasetFilter{
				{Pattern: "zroot", Recursive: &filterRecursive},
				{Pattern: "zroot", Exclude: true, Recursive: &filterNotRecursive},
			},
			all:       []string{"zroot", "zroot/foo", "zroot/bar", "zroot/baz"},
			expect:    []string{"zroot/foo", "zroot/bar", "zroot/baz"},
			recursive: map[string]bool{},
			parents: map[string]string{
				"zroot/foo": "zroot",
				"zroot/bar": "zroot",
				"zroot/baz": "zroot",
			},
			excluded: map[string]string{},
		},
		{
			name: "self-excluded with foo recursive",
			filter: []config.DatasetFilter{
				{Pattern: "zroot", Recursive: &filterRecursive},
				{Pattern: "zroot", Exclude: true, Recursive: &filterNotRecursive},
				{Pattern: "zroot/foo", Recursive: &filterRecursive},
			},
			all: []string{
				"zroot",
				"zroot/foo",
				"zroot/foo/bar",
				"zroot/foo/baz",
				"zroot/bar",
			},
			expect: []string{
				"zroot/foo",
				"zroot/foo/bar",
				"zroot/foo/baz",
				"zroot/bar",
			},
			recursive: map[string]bool{"zroot/foo": true},
			parents: map[string]string{
				"zroot/foo/bar": "zroot/foo",
				"zroot/foo/baz": "zroot/foo",
				"zroot/bar":     "zroot",
			},
			excluded: map[string]string{},
		},
		{
			name: "shell pattern",
			filter: []config.DatasetFilter{
				{Pattern: "zroot/*", Shell: true},
			},
			all:       []string{"zroot", "zroot/foo", "zroot/bar", "zroot/baz"},
			expect:    []string{"zroot/foo", "zroot/bar", "zroot/baz"},
			recursive: map[string]bool{},
			parents:   map[string]string{},
			excluded:  map[string]string{},
		},
		{
			name: "shell pattern recursive",
			filter: []config.DatasetFilter{
				{Pattern: "zroot/*", Shell: true, Recursive: &filterRecursive},
			},
			all: []string{
				"zroot",
				"zroot/foo",
				"zroot/foo/bar",
				"zroot/foo/baz",
				"zroot/bar",
			},
			expect: []string{
				"zroot/foo",
				"zroot/foo/bar",
				"zroot/foo/baz",
				"zroot/bar",
			},
			recursive: map[string]bool{
				"zroot/foo": true,
				"zroot/bar": true,
			},
			parents: map[string]string{
				"zroot/foo/bar": "zroot/foo",
				"zroot/foo/baz": "zroot/foo",
			},
			excluded: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := filter.NewFromConfig(nil, tt.filter)
			require.NoError(t, err)
			require.NotNil(t, f)

			all := make([]*dataset.Path, len(tt.all))
			for i, name := range tt.all {
				p, err := dataset.NewPath(name)
				require.NoError(t, err)
				require.NotNil(t, p)
				all[i] = p
			}

			filtered, err := filterDatasets(t.Context(), f, all)
			require.NoError(t, err)

			got := make([]string, len(filtered))
			for i, p := range filtered {
				got[i] = p.ToString()
			}
			assert.Equal(t, tt.expect, got, "unexpected filtered")

			if tt.recursive != nil {
				for _, p := range filtered {
					name := p.ToString()
					assert.Equal(t, tt.recursive[name], p.Recursive(),
						"%s: unexpected Recursive", name)
				}
			}

			if tt.parents != nil {
				for _, p := range filtered {
					name := p.ToString()
					parent := p.RecursiveParent()
					if parent == nil {
						assert.Empty(t, tt.parents[name],
							"%s: unexpected RecursiveParent", name)
					} else {
						assert.Equal(t, tt.parents[name], parent.ToString(),
							"%s: unexpected RecursiveParent", name)
					}
				}
			}

			if tt.excluded != nil {
				for _, p := range filtered {
					name := p.ToString()
					assert.Equal(t, tt.excluded[name], p.ExcludedString(),
						"%s: unexpected ExcludedString", name)
				}
			}
		})
	}
}
