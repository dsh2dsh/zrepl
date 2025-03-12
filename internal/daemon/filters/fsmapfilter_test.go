package filters

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/zfs"
)

func TestDatasetFilter_Filter_filesystems(t *testing.T) {
	tests := []struct {
		name          string
		filesystems   config.FilesystemsFilter
		topPaths      []string
		recursiveRoot map[string]string

		// each entry is checked to match the filter's `pass` return value
		checkPass map[string]bool
	}{
		{
			name:        "default_no_match",
			filesystems: config.FilesystemsFilter{},
			checkPass: map[string]bool{
				"":      false,
				"foo":   false,
				"zroot": false,
			},
		},
		{
			name: "more_specific_path_has_precedence",
			filesystems: config.FilesystemsFilter{
				"tank<":         true,
				"tank/tmp<":     false,
				"tank/home/x<":  false,
				"tank/home/x/1": true,
			},
			topPaths: []string{"tank"},
			checkPass: map[string]bool{
				"zroot":         false,
				"tank":          true,
				"tank/tmp":      false,
				"tank/tmp/foo":  false,
				"tank/home/x":   false,
				"tank/home/y":   true,
				"tank/home/x/1": true,
				"tank/home/x/2": false,
			},
			recursiveRoot: map[string]string{
				"tank":          "tank",
				"tank/tmp":      "tank",
				"tank/tmp/foo":  "tank",
				"tank/home/x":   "tank",
				"tank/home/y":   "tank",
				"tank/home/x/1": "tank",
				"tank/home/x/2": "tank",
			},
		},
		{
			name: "precedence_of_specific_over_subtree_wildcard_on_same_path",
			filesystems: config.FilesystemsFilter{
				"tank/home/bob":  true,
				"tank/home/bob<": false,
			},
			topPaths: []string{"tank/home/bob"},
			checkPass: map[string]bool{
				"tank/home/bob":           true,
				"tank/home/bob/downloads": false,
			},
		},
		{
			name: "with shell patterns",
			filesystems: config.FilesystemsFilter{
				"tank/home</*/foo": true,
				"tank/home/mark<":  false,
			},
			topPaths: []string{"tank/home"},
			checkPass: map[string]bool{
				"tank/home":              false,
				"tank/home/bob":          false,
				"tank/home/bob/foo":      true,
				"tank/home/alice/foo":    true,
				"tank/home/john/foo/bar": false,
				"tank/home/john/bar":     false,
				"tank/home/mark/foo":     false,
			},
			recursiveRoot: map[string]string{
				"tank/home/bob/foo":   "tank/home",
				"tank/home/alice/foo": "tank/home",
				"tank/home/mark/foo":  "tank/home",
			},
		},
		{
			name:        "include first level",
			filesystems: config.FilesystemsFilter{"test/app</*": true},
			topPaths:    []string{"test/app"},
			checkPass: map[string]bool{
				"test/app/1":       true,
				"test/app/1/cache": false,
				"test/app/2":       true,
				"test/app/2/cache": false,
			},
			recursiveRoot: map[string]string{
				"test/app/1": "test/app",
				"test/app/2": "test/app",
			},
		},
		{
			name:        "exclude by shell pattern",
			filesystems: config.FilesystemsFilter{"test/app</*/cache": false},
			topPaths:    []string{"test/app"},
			checkPass: map[string]bool{
				"test/app/1":       true,
				"test/app/1/cache": false,
				"test/app/2":       true,
				"test/app/2/cache": false,
			},
			recursiveRoot: map[string]string{
				"test/app/1":       "test/app",
				"test/app/1/cache": "test/app",
				"test/app/2":       "test/app",
				"test/app/2/cache": "test/app",
			},
		},
		{
			name:        "match all",
			filesystems: config.FilesystemsFilter{"<": true},
			checkPass: map[string]bool{
				"test":             true,
				"test/app":         true,
				"test/app/2":       true,
				"test/app/2/cache": true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := NewFromConfig(tt.filesystems, nil)
			require.NoError(t, err)
			require.NotNil(t, f)

			n, seq := f.TopFilesystems()
			topPaths := slices.Collect(seq)
			assert.Equal(t, len(topPaths), n)
			assert.Equal(t, tt.topPaths, topPaths)

			for p, checkPass := range tt.checkPass {
				zp, err := zfs.NewDatasetPath(p)
				require.NoError(t, err)
				require.NotNil(t, zp)

				recursiveRoot, pass, err := f.Filter2(zp)
				require.NoError(t, err)
				pass2, err2 := f.Filter(zp)
				require.NoError(t, err2)
				require.Equal(t, pass, pass2)
				assert.Equal(t, checkPass, pass, "pattern: %s", p)

				if tt.recursiveRoot == nil {
					assert.Nil(t, recursiveRoot, "pattern: %s", p)
					continue
				}

				path := tt.recursiveRoot[p]
				if path == "" {
					assert.Nil(t, recursiveRoot, "pattern: %s", p)
				} else {
					require.NotNil(t, recursiveRoot)
					assert.Equal(t, path, recursiveRoot.ToString())
				}
			}
		})
	}
}

func TestNoFilter(t *testing.T) {
	f, err := NoFilter()
	require.NoError(t, err)
	require.NotNil(t, f)

	tests := []string{
		"test/foo/bar/baz",
		"test/foo/bar",
		"test/foo",
		"test",
	}

	for _, s := range tests {
		t.Run(s, func(t *testing.T) {
			path, err := zfs.NewDatasetPath(s)
			require.NoError(t, err)
			require.NotNil(t, path)

			ok, err := f.Filter(path)
			require.NoError(t, err)
			assert.True(t, ok)
		})
	}
}

func TestDatasetFilter_Filter_datasets(t *testing.T) {
	tests := []struct {
		name          string
		datasets      []config.DatasetFilter
		topPaths      []string
		recursiveRoot map[string]string

		// each entry is checked to match the filter's `pass` return value
		checkPass map[string]bool
	}{
		{
			name: "default_no_match",
			checkPass: map[string]bool{
				"":      false,
				"foo":   false,
				"zroot": false,
			},
		},
		{
			name: "more_specific_path_has_precedence",
			datasets: []config.DatasetFilter{
				{Pattern: "tank", Recursive: true},
				{Pattern: "tank/tmp", Recursive: true, Exclude: true},
				{Pattern: "tank/home/x", Recursive: true, Exclude: true},
				{Pattern: "tank/home/x/1"},
			},
			topPaths: []string{"tank"},
			checkPass: map[string]bool{
				"zroot":         false,
				"tank":          true,
				"tank/tmp":      false,
				"tank/tmp/foo":  false,
				"tank/home/x":   false,
				"tank/home/y":   true,
				"tank/home/x/1": true,
				"tank/home/x/2": false,
			},
			recursiveRoot: map[string]string{
				"tank":          "tank",
				"tank/tmp":      "tank",
				"tank/tmp/foo":  "tank",
				"tank/home/x":   "tank",
				"tank/home/y":   "tank",
				"tank/home/x/1": "tank",
				"tank/home/x/2": "tank",
			},
		},
		{
			name: "precedence_of_specific_over_subtree_wildcard_on_same_path",
			datasets: []config.DatasetFilter{
				{Pattern: "tank/home/bob", Recursive: true, Exclude: true},
				{Pattern: "tank/home/bob"},
			},
			topPaths: []string{"tank/home/bob"},
			checkPass: map[string]bool{
				"tank/home/bob":           true,
				"tank/home/bob/downloads": false,
			},
		},
		{
			name: "with shell patterns",
			datasets: []config.DatasetFilter{
				{Pattern: "tank/home/*/foo", Shell: true},
				{Pattern: "tank/home/mark", Recursive: true, Exclude: true},
			},
			topPaths: []string{"tank/home"},
			checkPass: map[string]bool{
				"tank/home":              false,
				"tank/home/bob":          false,
				"tank/home/bob/foo":      true,
				"tank/home/alice/foo":    true,
				"tank/home/john/foo/bar": false,
				"tank/home/john/bar":     false,
				"tank/home/mark/foo":     false,
			},
		},
		{
			name: "include first level",
			datasets: []config.DatasetFilter{
				{Pattern: "test/app/*", Shell: true},
			},
			topPaths: []string{"test/app"},
			checkPass: map[string]bool{
				"test/app/1":       true,
				"test/app/1/cache": false,
				"test/app/2":       true,
				"test/app/2/cache": false,
			},
		},
		{
			name: "exclude by shell pattern",
			datasets: []config.DatasetFilter{
				{Pattern: "test/app", Recursive: true},
				{Pattern: "test/app/*/cache", Shell: true, Exclude: true},
			},
			topPaths: []string{"test/app"},
			checkPass: map[string]bool{
				"test/app/1":       true,
				"test/app/1/cache": false,
				"test/app/2":       true,
				"test/app/2/cache": false,
			},
			recursiveRoot: map[string]string{
				"test/app/1":       "test/app",
				"test/app/1/cache": "test/app",
				"test/app/2":       "test/app",
				"test/app/2/cache": "test/app",
			},
		},
		{
			name:     "match all",
			datasets: []config.DatasetFilter{{Recursive: true}},
			checkPass: map[string]bool{
				"test":             true,
				"test/app":         true,
				"test/app/2":       true,
				"test/app/2/cache": true,
			},
		},
		{
			name: "desktop",
			datasets: []config.DatasetFilter{
				{Pattern: "zroot/ROOT/default"},
				{Pattern: "zroot/usr/home"},
			},
			topPaths:  []string{"zroot/ROOT/default", "zroot/usr/home"},
			checkPass: map[string]bool{},
		},
		{
			name: "bastille",
			datasets: []config.DatasetFilter{
				{Pattern: "zroot/ROOT/default"},
				{Pattern: "zroot/bastille/jails/*/root", Shell: true},
				{Pattern: "zroot/usr/home"},
			},
			topPaths: []string{
				"zroot/ROOT/default",
				"zroot/bastille/jails",
				"zroot/usr/home",
			},
			checkPass: map[string]bool{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := NewFromConfig(nil, tt.datasets)
			require.NoError(t, err)
			require.NotNil(t, f)

			for p, checkPass := range tt.checkPass {
				zp, err := zfs.NewDatasetPath(p)
				require.NoError(t, err)
				require.NotNil(t, zp)

				n, seq := f.TopFilesystems()
				topPaths := slices.Collect(seq)
				assert.Equal(t, len(topPaths), n)
				assert.Equal(t, tt.topPaths, topPaths)

				recursiveRoot, pass, err := f.Filter2(zp)
				require.NoError(t, err)
				pass2, err2 := f.Filter(zp)
				require.NoError(t, err2)
				require.Equal(t, pass, pass2)
				assert.Equal(t, checkPass, pass, "pattern: %s", p)

				if tt.recursiveRoot == nil {
					assert.Nil(t, recursiveRoot)
					continue
				}

				path := tt.recursiveRoot[p]
				if path == "" {
					assert.Nil(t, recursiveRoot)
				} else {
					require.NotNil(t, recursiveRoot)
					assert.Equal(t, path, recursiveRoot.ToString())
				}
			}
		})
	}
}
