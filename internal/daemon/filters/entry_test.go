package filters

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/zfs"
)

func Test_shellBaseDir(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		want    string
	}{
		{
			name:    "pool name",
			pattern: "zroot",
			want:    "zroot",
		},
		{
			name:    "pool name with pattern",
			pattern: "zroot[ab]",
		},
		{
			name:    "filesystem",
			pattern: "zroot/foo",
			want:    "zroot/foo",
		},
		{
			name:    "subdirs",
			pattern: "zroot/foo/*",
			want:    "zroot/foo",
		},
		{
			name:    "foo like filesystems",
			pattern: "zroot/foo*",
			want:    "zroot",
		},
		{
			name:    "many subdirs",
			pattern: "zroot/foo/*/?",
			want:    "zroot/foo",
		},
		{
			name:    "middle pattern",
			pattern: "zroot/*/bar",
			want:    "zroot",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, shellBaseDir(tt.pattern))
		})
	}
}

func Test_filterItem_ParentFilesystem(t *testing.T) {
	tests := []struct {
		name string
		item config.DatasetFilter
		want string
	}{
		{name: "empty"},
		{
			name: "include fs",
			item: config.DatasetFilter{Pattern: "zroot/usr/home"},
			want: "zroot/usr/home",
		},
		{
			name: "exclude fs",
			item: config.DatasetFilter{Pattern: "zroot/usr/home", Exclude: true},
		},
		{
			name: "include pattern",
			item: config.DatasetFilter{Pattern: "zroot/home/*", Shell: true},
			want: "zroot/home",
		},
		{
			name: "exclude pattern",
			item: config.DatasetFilter{
				Pattern: "zroot/*/home",
				Shell:   true,
				Exclude: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fi, err := NewItem(tt.item)
			require.NoError(t, err)
			require.NotNil(t, fi)
			path := fi.ParentFilesystem()
			if tt.want == "" {
				assert.Nil(t, path)
			} else {
				require.NotNil(t, path)
				assert.Equal(t, tt.want, path.ToString())
			}
		})
	}
}

func Test_filterItem_HasPrefix(t *testing.T) {
	tests := []struct {
		name   string
		item   config.DatasetFilter
		prefix string
		want   bool
	}{
		{
			name:   "filesystem has prefix",
			item:   config.DatasetFilter{Pattern: "zroot/home/foo", Shell: true},
			prefix: "zroot",
			want:   true,
		},
		{
			name:   "filesystem hasnt prefix",
			item:   config.DatasetFilter{Pattern: "zroot/home/foo", Shell: true},
			prefix: "zroot/foo",
		},
		{
			name:   "shell pattern has prefix",
			item:   config.DatasetFilter{Pattern: "zroot/home/*/foo", Shell: true},
			prefix: "zroot",
			want:   true,
		},
		{
			name:   "shell pattern hasnt prefix",
			item:   config.DatasetFilter{Pattern: "zroot/home/*/foo", Shell: true},
			prefix: "zroot/foo",
		},
		{
			name:   "shell pattern pool",
			item:   config.DatasetFilter{Pattern: "z*", Shell: true},
			prefix: "zroot/foo",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item, err := NewItem(tt.item)
			require.NoError(t, err)
			require.NotNil(t, item)

			prefixPath, err := zfs.NewDatasetPath(tt.prefix)
			require.NoError(t, err)
			require.NotNil(t, prefixPath)

			assert.Equal(t, tt.want, item.HasPrefix(prefixPath))
		})
	}
}
