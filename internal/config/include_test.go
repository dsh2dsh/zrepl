package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIncludeYAML(t *testing.T) {
	c, err := ParseConfig("testdata/include_keys.yaml")
	require.NoError(t, err)
	assert.Equal(t, []AuthKey{
		{
			Name: "test",
			Key:  "ThBKqH8aZojsKF8FPdKbClQCJPPb2+Abpv1Nl2EQaaU=",
		},
	}, c.Keys)
}

func TestIncludeYAML_notExist(t *testing.T) {
	var c Config
	err := includeYAML("testdata/zrepl.yaml", "notexists.yaml", &c.Keys)
	require.Error(t, err)
}
