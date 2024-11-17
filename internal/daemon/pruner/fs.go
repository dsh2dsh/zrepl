package pruner

import (
	"sync"

	"github.com/dsh2dsh/zrepl/internal/pruning"
)

type fs struct {
	path string

	// permanent error during planning
	planErr        error
	planErrContext string

	// if != "", the fs was skipped for planning and the field
	// contains the reason
	skipReason FSSkipReason

	// snapshots presented by target
	// (type snapshot)
	snaps []pruning.Snapshot
	// destroy list returned by pruning.PruneSnapshots(snaps)
	// (type snapshot)
	destroyList []pruning.Snapshot

	mtx sync.RWMutex

	// only during Exec state, also used by execQueue
	execErrLast error
}

type FSSkipReason string

const (
	NotSkipped                   = ""
	SkipPlaceholder              = "filesystem is placeholder"
	SkipNoCorrespondenceOnSender = "filesystem has no correspondence on sender"
)

func (r FSSkipReason) NotSkipped() bool {
	return r == NotSkipped
}

func (f *fs) Report() FSReport {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	r := FSReport{}
	r.Filesystem = f.path
	r.SkipReason = f.skipReason
	if !r.SkipReason.NotSkipped() {
		return r
	}

	if f.planErr != nil {
		r.LastError = f.planErr.Error()
	} else if f.execErrLast != nil {
		r.LastError = f.execErrLast.Error()
	}

	r.SnapshotList = make([]SnapshotReport, len(f.snaps))
	for i, snap := range f.snaps {
		r.SnapshotList[i] = snap.(*snapshot).Report()
	}

	r.DestroyList = make([]SnapshotReport, len(f.destroyList))
	for i, snap := range f.destroyList {
		r.DestroyList[i] = snap.(*snapshot).Report()
	}

	return r
}
