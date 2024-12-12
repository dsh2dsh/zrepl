package pruner

import (
	"cmp"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"

	"github.com/dsh2dsh/zrepl/internal/logger"
	"github.com/dsh2dsh/zrepl/internal/pruning"
	"github.com/dsh2dsh/zrepl/internal/replication/logic/pdu"
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

func (self *fs) Build(a *args, tfs *pdu.Filesystem, target Target,
	sender Sender, needsReplicated bool,
) {
	ctx := a.ctx
	l := GetLogger(ctx).With(slog.String("fs", tfs.Path))
	l.Debug("plan filesystem")

	pfsPlanErrAndLog := func(err error, message string) {
		t := fmt.Sprintf("%T", err)
		self.planErr = err
		self.planErrContext = message
		logger.WithError(l.With(slog.String("orig_err_type", t)), err,
			message+": plan error, skipping filesystem")
	}

	req := pdu.ListFilesystemVersionsReq{Filesystem: tfs.Path}
	tfsvsres, err := target.ListFilesystemVersions(ctx, &req)
	if err != nil {
		pfsPlanErrAndLog(err, "cannot list filesystem versions")
		return
	}
	tfsvs := tfsvsres.GetVersions()
	// no progress here since we could run in a live-lock (must have used target
	// AND receiver before progress)

	// scan from older to newer, all snapshots older than cursor are interpreted
	// as replicated
	slices.SortFunc(tfsvs, func(a, b *pdu.FilesystemVersion) int {
		return cmp.Compare(a.CreateTXG, b.CreateTXG)
	})
	self.snaps = make([]pruning.Snapshot, 0, len(tfsvs))

	var cursorGuid uint64
	var beforeCursor bool
	if needsReplicated {
		req := pdu.ReplicationCursorReq{Filesystem: tfs.Path}
		resp, err := sender.ReplicationCursor(ctx, &req)
		if err != nil {
			pfsPlanErrAndLog(err, "cannot get replication cursor bookmark")
			return
		} else if resp.GetNotexist() {
			err := errors.New(
				"replication cursor bookmark does not exist (one successful replication is required before pruning works)")
			pfsPlanErrAndLog(err, "")
			return
		}
		cursorGuid = resp.GetGuid()
		beforeCursor = containsGuid(tfsvs, cursorGuid)
	}

	for _, tfsv := range tfsvs {
		if tfsv.Type != pdu.FilesystemVersion_Snapshot {
			continue
		}
		creation, err := tfsv.CreationAsTime()
		if err != nil {
			err := fmt.Errorf("%s: %w", tfsv.RelName(), err)
			pfsPlanErrAndLog(err, "fs version with invalid creation date")
			return
		}
		s := &snapshot{date: creation, fsv: tfsv}
		// note that we cannot use CreateTXG because target and receiver could be
		// on different pools
		if needsReplicated {
			atCursor := tfsv.Guid == cursorGuid
			beforeCursor = beforeCursor && !atCursor
			s.replicated = beforeCursor ||
				(a.considerSnapAtCursorReplicated && atCursor)
		}
		self.snaps = append(self.snaps, s)
	}

	if needsReplicated && beforeCursor {
		err := errors.New("prune target has no snapshot that corresponds to sender replication cursor bookmark")
		pfsPlanErrAndLog(err, "")
		return
	}

	// Apply prune rules
	self.destroyList = pruning.PruneSnapshots(ctx, self.snaps, a.rules)
}
