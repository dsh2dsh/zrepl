package zfs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestZFSGetFilesystemPlaceholderState_doesntExist(t *testing.T) {
	t.SkipNow()
	fs, err := NewDatasetPath("zdisk/zrepl/doesntexist")
	require.NoError(t, err)

	state, err := ZFSGetFilesystemPlaceholderState(t.Context(), fs)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.False(t, state.FSExists)
}
