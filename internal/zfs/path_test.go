package zfs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatasetPathTrimNPrefixComps(t *testing.T) {
	p, err := NewDatasetPath("foo/bar/a/b")
	require.NoError(t, err)
	p.TrimNPrefixComps(2)
	assert.True(t, p.Equal(toDatasetPath("a/b")))
	p.TrimNPrefixComps((2))
	assert.True(t, p.Empty())
	p.TrimNPrefixComps((1))
	assert.True(t, p.Empty(), "empty trimming shouldn't do harm")
}

func toDatasetPath(s string) *DatasetPath {
	p, err := NewDatasetPath(s)
	if err != nil {
		panic(err)
	}
	return p
}
