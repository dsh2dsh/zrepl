package endpoint

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/dsh2dsh/zrepl/internal/zfs"
)

// JobID instances returned by MakeJobID() guarantee their JobID.String()
// can be used in ZFS dataset names and hold tags.
type JobID struct {
	jid string
}

func MakeJobID(s string) (JobID, error) {
	if len(s) == 0 {
		return JobID{}, errors.New("must not be empty string")
	}

	if err := zfs.ComponentNamecheck(s); err != nil {
		return JobID{}, fmt.Errorf("must be usable as a dataset path component: %w", err)
	}

	if _, err := tentativeReplicationCursorBookmarkNameImpl("pool/ds", 0xface601d, s); err != nil {
		// note that this might still fail due to total maximum name length, but we can't enforce that
		return JobID{}, fmt.Errorf("must be usable for a tentative replication cursor bookmark: %w", err)
	}

	if _, err := stepHoldTagImpl(s); err != nil {
		return JobID{}, fmt.Errorf("must be usable for a step hold tag: %w", err)
	}

	if _, err := lastReceivedHoldImpl(s); err != nil {
		return JobID{}, fmt.Errorf("must be usable as a last-received-hold tag: %w", err)
	}

	// FIXME replication cursor bookmark name

	_, err := zfs.NewDatasetPath(s)
	if err != nil {
		return JobID{}, fmt.Errorf("must be usable in a ZFS dataset path: %w", err)
	}

	return JobID{s}, nil
}

func MustMakeJobID(s string) JobID {
	jid, err := MakeJobID(s)
	if err != nil {
		panic(err)
	}
	return jid
}

func (j JobID) expectInitialized() {
	if j.jid == "" {
		panic("use of uninitialized JobID")
	}
}

func (j JobID) String() string {
	j.expectInitialized()
	return j.jid
}

var (
	_ json.Marshaler   = JobID{}
	_ json.Unmarshaler = (*JobID)(nil)
)

func (j JobID) MarshalJSON() ([]byte, error) { return json.Marshal(j.jid) }

func (j *JobID) UnmarshalJSON(b []byte) error {
	if err := json.Unmarshal(b, &j.jid); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	return nil
}

func (j JobID) MustValidate() { j.expectInitialized() }
