package env

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse_defaults(t *testing.T) {
	want := Values
	require.NoError(t, Parse())
	assert.Equal(t, &want, &Values)
}

func TestParse_Setenv(t *testing.T) {
	want := Values
	want.ReplicationMaxAttempts *= 10
	t.Setenv("ZREPL_REPLICATION_MAX_ATTEMPTS",
		strconv.FormatInt(int64(want.ReplicationMaxAttempts), 10))
	require.NoError(t, Parse())
	assert.Equal(t, &want, &Values)
}
