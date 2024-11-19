package pruner

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	"github.com/dsh2dsh/zrepl/internal/daemon/logging"
	"github.com/dsh2dsh/zrepl/internal/logger"
	"github.com/dsh2dsh/zrepl/internal/pruning"
	"github.com/dsh2dsh/zrepl/internal/replication/logic/pdu"
)

// The sender in the replication setup. The pruner uses the Sender to determine
// which of the Target's filesystems need to be pruned. Also, it asks the Sender
// about the replication cursor of each filesystem to enable the
// 'not_replicated' pruning rule.
//
// Try to keep it compatible with endpoint.Endpoint.
type Sender interface {
	ReplicationCursor(ctx context.Context, req *pdu.ReplicationCursorReq,
	) (*pdu.ReplicationCursorRes, error)
	ListFilesystems(ctx context.Context) (*pdu.ListFilesystemRes, error)
}

// The pruning target, i.e., on which snapshots are destroyed.
// This can be a replication sender or receiver.
//
// Try to keep it compatible with endpoint.Endpoint.
type Target interface {
	ListFilesystems(ctx context.Context) (*pdu.ListFilesystemRes, error)
	ListFilesystemVersions(ctx context.Context,
		req *pdu.ListFilesystemVersionsReq,
	) (*pdu.ListFilesystemVersionsRes, error)
	DestroySnapshots(ctx context.Context, req *pdu.DestroySnapshotsReq,
	) (*pdu.DestroySnapshotsRes, error)
}

type contextKey int

const (
	contextKeyPruneSide contextKey = 1 + iota
)

func GetLogger(ctx context.Context) *logger.Logger {
	pruneSide := ctx.Value(contextKeyPruneSide).(string)
	return logging.GetLogger(ctx, logging.SubsysPruning).
		WithField("prune_side", pruneSide)
}

type args struct {
	concurrency                    int
	ctx                            context.Context
	target                         Target
	sender                         Sender
	rules                          []pruning.KeepRule
	retryWait                      time.Duration
	considerSnapAtCursorReplicated bool
	promPruneSecs                  prometheus.Observer
}

type Pruner struct {
	args args

	mtx sync.RWMutex

	state     State
	startedAt time.Time

	// State PlanErr
	err error

	// State Exec
	execQueue *execQueue
}

type updater func(func(*Pruner))

func (p *Pruner) Prune() {
	if len(p.args.rules) == 0 {
		l := GetLogger(p.args.ctx)
		l.Info("skip pruning, because no keep rules configured")
		p.state = Done
		return
	}
	p.prune(p.args)
}

func (p *Pruner) prune(args args) {
	u := func(f func(*Pruner)) {
		p.mtx.Lock()
		defer p.mtx.Unlock()
		f(p)
	}
	// TODO support automatic retries
	//
	// It is advisable to merge this code with package replication/driver before
	// That will likely require re-modelling struct fs like
	// replication/driver.attempt, including figuring out how to resume a plan
	// after being interrupted by network errors The non-retrying code in this
	// package should move straight to replication/logic.
	doOneAttempt(&args, u)
}

func (p *Pruner) Report() *Report {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	r := Report{State: p.state.String(), StartedAt: p.startedAt}

	if p.err != nil {
		r.Error = p.err.Error()
	}

	if p.execQueue != nil {
		r.Pending, r.Completed = p.execQueue.Report()
	}

	return &r
}

func (p *Pruner) State() State {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	return p.state
}

func doOneAttempt(a *args, u updater) {
	ctx, target, sender := a.ctx, a.target, a.sender

	sfss, tfss, err := listFilesystems(ctx, sender, target)
	if err != nil {
		u(func(p *Pruner) {
			p.state = PlanErr
			p.err = err
		})
		return
	}

	pfss := make([]*fs, len(tfss))
	needsReplicated := containsNotReplicated(a.rules)
	g := new(errgroup.Group)
	g.SetLimit(a.concurrency)

	for i, tfs := range tfss {
		if ctx.Err() != nil {
			break
		}
		l := GetLogger(ctx).WithField("fs", tfs.Path)
		l.Debug("plan filesystem")

		pfs := &fs{path: tfs.Path}
		pfss[i] = pfs

		if tfs.GetIsPlaceholder() {
			pfs.skipReason = SkipPlaceholder
			l.WithField("skip_reason", pfs.skipReason).Debug("skipping filesystem")
			continue
		} else if sfs := sfss[tfs.GetPath()]; sfs == nil {
			pfs.skipReason = SkipNoCorrespondenceOnSender
			l.WithField("skip_reason", pfs.skipReason).
				WithField("sfs", sfs.GetPath()).Debug("skipping filesystem")
			continue
		}

		g.Go(func() error {
			pfs.Build(a, tfs, target, sender, needsReplicated)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		u(func(p *Pruner) {
			p.state = PlanErr
			p.err = err
		})
		return
	} else if ctx.Err() != nil {
		u(func(p *Pruner) {
			p.state = PlanErr
			p.err = context.Cause(ctx)
		})
		return
	}

	u(func(pruner *Pruner) {
		pruner.execQueue = newExecQueue(len(pfss))
		for _, pfs := range pfss {
			pruner.execQueue.Put(pfs, nil, false)
		}
		pruner.state = Exec
	})

	for {
		var pfs *fs
		u(func(pruner *Pruner) {
			pfs = pruner.execQueue.Pop()
		})
		if pfs == nil {
			break
		}
		doOneAttemptExec(a, u, pfs)
	}

	var rep *Report
	{
		// must not hold lock for report
		var pruner *Pruner
		u(func(p *Pruner) {
			pruner = p
		})
		rep = pruner.Report()
	}

	u(func(p *Pruner) {
		if len(rep.Pending) > 0 {
			panic("queue should not have pending items at this point")
		}
		hadErr := false
		for _, fsr := range rep.Completed {
			hadErr = hadErr || fsr.SkipReason.NotSkipped() && fsr.LastError != ""
		}
		if hadErr {
			p.state = ExecErr
		} else {
			p.state = Done
		}
	})
}

func listFilesystems(ctx context.Context, sender Sender, target Target,
) (sfss map[string]*pdu.Filesystem, tfss []*pdu.Filesystem, _ error) {
	g := new(errgroup.Group)
	g.Go(func() error {
		sfssres, err := sender.ListFilesystems(ctx)
		if err != nil {
			return err
		}
		sfss = make(map[string]*pdu.Filesystem, len(sfssres.Filesystems))
		for _, sfs := range sfssres.Filesystems {
			sfss[sfs.GetPath()] = sfs
		}
		return nil
	})

	g.Go(func() error {
		tfssres, err := target.ListFilesystems(ctx)
		if err != nil {
			return err
		}
		tfss = tfssres.Filesystems
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, nil, err //nolint:wrapcheck // it's our error
	}
	return sfss, tfss, nil
}

func containsNotReplicated(rules []pruning.KeepRule) bool {
	i := slices.IndexFunc(rules, func(r pruning.KeepRule) bool {
		_, ok := r.(*pruning.KeepNotReplicated)
		return ok
	})
	return i >= 0
}

func containsGuid(snapshots []*pdu.FilesystemVersion, guid uint64) bool {
	i := slices.IndexFunc(snapshots,
		func(s *pdu.FilesystemVersion) bool {
			return s.Type == pdu.FilesystemVersion_Snapshot && s.Guid == guid
		})
	return i >= 0
}

// attempts to exec pfs, puts it back into the queue with the result
func doOneAttemptExec(a *args, u updater, pfs *fs) {
	destroyList := make([]*pdu.FilesystemVersion, len(pfs.destroyList))
	for i := range destroyList {
		destroyList[i] = pfs.destroyList[i].(*snapshot).fsv
		GetLogger(a.ctx).
			WithField("fs", pfs.path).
			WithField("destroy_snap", destroyList[i].Name).
			Debug("policy destroys snapshot")
	}

	req := pdu.DestroySnapshotsReq{
		Filesystem: pfs.path,
		Snapshots:  destroyList,
	}
	GetLogger(a.ctx).WithField("fs", pfs.path).Debug("destroying snapshots")
	res, err := a.target.DestroySnapshots(a.ctx, &req)
	if err != nil {
		u(func(pruner *Pruner) {
			pruner.execQueue.Put(pfs, err, false)
		})
		return
	}

	// check if all snapshots were destroyed
	destroyResults := make(map[string]*pdu.DestroySnapshotRes)
	for _, fsres := range res.Results {
		destroyResults[fsres.Snapshot.Name] = fsres
	}

	err = nil
	destroyFails := make([]*pdu.DestroySnapshotRes, 0)
	for _, reqDestroy := range destroyList {
		res, ok := destroyResults[reqDestroy.Name]
		if !ok {
			err = fmt.Errorf("missing destroy-result for %s", reqDestroy.RelName())
			break
		} else if res.Error != "" {
			destroyFails = append(destroyFails, res)
		}
	}

	if err == nil && len(destroyFails) > 0 {
		names := make([]string, len(destroyFails))
		pairs := make([]string, len(destroyFails))
		allSame := true
		lastMsg := destroyFails[0].Error
		for i := 0; i < len(destroyFails); i++ {
			allSame = allSame && destroyFails[i].Error == lastMsg
			relname := destroyFails[i].Snapshot.RelName()
			names[i] = relname
			pairs[i] = fmt.Sprintf("(%s: %s)", relname, destroyFails[i].Error)
		}
		if allSame {
			err = fmt.Errorf("destroys failed %s: %s",
				strings.Join(names, ", "), lastMsg)
		} else {
			err = fmt.Errorf("destroys failed: %s", strings.Join(pairs, ", "))
		}
	}

	u(func(pruner *Pruner) { pruner.execQueue.Put(pfs, err, err == nil) })
	if err != nil {
		GetLogger(a.ctx).WithError(err).
			Error("target could not destroy snapshots")
	}
}
