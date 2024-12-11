package config

import (
	"io/fs"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppendYAML_keys(t *testing.T) {
	c, err := ParseConfig("testdata/include_keys.yaml")
	require.NoError(t, err)
	assert.Equal(t, []AuthKey{
		{
			Name: "test",
			Key:  "ThBKqH8aZojsKF8FPdKbClQCJPPb2+Abpv1Nl2EQaaU=",
		},
	}, c.Keys)
}

func TestAppendYAML_notExist(t *testing.T) {
	var c Config
	_, err := appendYAML("testdata/zrepl.yaml", "notexists.yaml", c.Keys)
	t.Log(err)
	require.ErrorIs(t, err, fs.ErrNotExist)
}

func TestAppendYAML_jobs(t *testing.T) {
	c, err := ParseConfig("testdata/include_jobs.yaml")
	require.NoError(t, err)
	assert.Len(t, c.Jobs, 2)
	assert.Equal(t, "bar", c.Jobs[0].Name())
	assert.Equal(t, "foo", c.Jobs[1].Name())
}
