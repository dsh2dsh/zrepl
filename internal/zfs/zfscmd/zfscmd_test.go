package zfscmd

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCmd_WithEnv(t *testing.T) {
	testEnv := map[string]string{"FOO": "BAR"}
	echoCmd := []string{"sh", "-c", "echo -n $FOO $BAR"}

	t.Setenv("BAR", "BAZ")
	cmd := CommandContext(t.Context(), echoCmd[0], echoCmd[1:]...).
		WithEnv(testEnv)
	var output bytes.Buffer
	cmd.setStdio(Stdio{Stdout: &output, Stderr: &output})

	require.NoError(t, cmd.Start())
	require.NoError(t, cmd.Wait())
	assert.Equal(t, "BAR BAZ", output.String())

	for _, c := range cmd.cmds {
		assert.Contains(t, c.Env, "BAR=BAZ", c.String())
		assert.Contains(t, c.Env, "FOO=BAR", c.String())
	}
}
