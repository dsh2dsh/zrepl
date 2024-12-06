package job

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStatus_MarshalJSON(t *testing.T) {
	st := Status{
		Err:      "oops, something is wrong",
		NextCron: time.Now().UTC(),

		Type:        "push",
		JobSpecific: &ActiveSideStatus{CronSpec: "20 * * * *"},
	}

	b, err := json.Marshal(&st)
	require.NoError(t, err)
	t.Log(string(b))

	var st2 Status
	require.NoError(t, json.Unmarshal(b, &st2))
	assert.Equal(t, &st, &st2)
}
