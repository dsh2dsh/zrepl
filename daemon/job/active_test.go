package job

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dsh2dsh/zrepl/endpoint"
	"github.com/dsh2dsh/zrepl/transport"
)

func TestFakeActiveSideDirectMethodInvocationClientIdentityDoesNotPassValidityTest(t *testing.T) {
	jobid, err := endpoint.MakeJobID("validjobname")
	require.NoError(t, err)
	clientIdentity := FakeActiveSideDirectMethodInvocationClientIdentity(jobid)
	t.Logf("%v", clientIdentity)
	err = transport.ValidateClientIdentity(clientIdentity)
	assert.Error(t, err)
}
