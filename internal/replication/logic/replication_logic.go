package logic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	"github.com/dsh2dsh/zrepl/internal/logger"
	"github.com/dsh2dsh/zrepl/internal/replication/driver"
	. "github.com/dsh2dsh/zrepl/internal/replication/logic/diff"
	"github.com/dsh2dsh/zrepl/internal/replication/logic/pdu"
	"github.com/dsh2dsh/zrepl/internal/replication/report"
	"github.com/dsh2dsh/zrepl/internal/zfs"
)

// Endpoint represents one side of the replication.
//
// An endpoint is either in Sender or Receiver mode, represented by the correspondingly
// named interfaces defined in this package.
type Endpoint interface {
	// Does not include placeholder filesystems
	ListFilesystems(ctx context.Context) (*pdu.ListFilesystemRes, error)
	ListFilesystemVersions(ctx context.Context, req *pdu.ListFilesystemVersionsReq) (*pdu.ListFilesystemVersionsRes, error)
	DestroySnapshots(ctx context.Context, req *pdu.DestroySnapshotsReq) (*pdu.DestroySnapshotsRes, error)
	WaitForConnectivity(ctx context.Context) error
}

type Sender interface {
	Endpoint
	// If a non-nil io.ReadCloser is returned, it is guaranteed to be closed before
	// any next call to the parent github.com/dsh2dsh/zrepl/replication.Endpoint.
	// If the send request is for dry run the io.ReadCloser will be nil
	Send(ctx context.Context, r *pdu.SendReq) (*pdu.SendRes, io.ReadCloser, error)
	SendDry(ctx context.Context, r *pdu.SendDryReq) (*pdu.SendDryRes, error)
	SendCompleted(ctx context.Context, r *pdu.SendCompletedReq) error
	ReplicationCursor(ctx context.Context, req *pdu.ReplicationCursorReq) (*pdu.ReplicationCursorRes, error)
}

type Receiver interface {
	Endpoint
	// Receive sends r and sendStream (the latter containing a ZFS send stream)
	// to the parent github.com/dsh2dsh/zrepl/replication.Endpoint.
	Receive(ctx context.Context, req *pdu.ReceiveReq, receive io.ReadCloser) error
}

type Planner struct {
	sender   Sender
	receiver Receiver
	policy   PlannerPolicy

	promSecsPerState    *prometheus.HistogramVec // labels: state
	promBytesReplicated *prometheus.CounterVec   // labels: filesystem
}

func (p *Planner) Plan(ctx context.Context) ([]driver.FS, error) {
	fss, err := p.doPlanning(ctx)
	if err != nil {
		return nil, err
	}
	dfss := make([]driver.FS, len(fss))
	for i := range dfss {
		dfss[i] = fss[i]
	}
	return dfss, nil
}

func (p *Planner) WaitForConnectivity(ctx context.Context) error {
	var wg sync.WaitGroup
	doPing := func(endpoint Endpoint, errOut *error) {
		defer wg.Done()
		err := endpoint.WaitForConnectivity(ctx)
		if err != nil {
			*errOut = err
		} else {
			*errOut = nil
		}
	}
	wg.Add(2)
	var senderErr, receiverErr error
	go doPing(p.sender, &senderErr)
	go doPing(p.receiver, &receiverErr)
	wg.Wait()
	switch {
	case senderErr == nil && receiverErr == nil:
		return nil
	case senderErr != nil && receiverErr != nil:
		if senderErr.Error() == receiverErr.Error() {
			return fmt.Errorf("sender and receiver are not reachable: %s",
				senderErr.Error())
		} else {
			return fmt.Errorf(
				"sender and receiver are not reachable:\n  sender: %s\n  receiver: %s",
				senderErr.Error(), receiverErr.Error())
		}
	default:
		var side string
		var err *error
		if senderErr != nil {
			side = "sender"
			err = &senderErr
		} else {
			side = "receiver"
			err = &receiverErr
		}
		return fmt.Errorf("%s is not reachable: %w", side, *err)
	}
}

type Filesystem struct {
	sender   Sender
	receiver Receiver
	policy   PlannerPolicy // immutable, it's .ReplicationConfig member is a pointer and copied into messages

	Path                 string             // compat
	receiverFS, senderFS *pdu.Filesystem    // receiverFS may be nil, senderFS never nil
	promBytesReplicated  prometheus.Counter // compat
}

func (f *Filesystem) EqualToPreviousAttempt(other driver.FS) bool {
	g, ok := other.(*Filesystem)
	if !ok {
		return false
	}
	// TODO: use GUIDs (issued by zrepl, not those from ZFS)
	return f.Path == g.Path
}

func (f *Filesystem) PlanFS(ctx context.Context, prefix string) ([]driver.Step,
	error,
) {
	steps, err := f.doPlanning(ctx, prefix)
	if err != nil {
		return nil, err
	}
	dsteps := make([]driver.Step, len(steps))
	for i := range dsteps {
		dsteps[i] = steps[i]
	}
	return dsteps, nil
}

func (f *Filesystem) ReportInfo() *report.FilesystemInfo {
	return &report.FilesystemInfo{Name: f.Path} // FIXME compat name
}

// caller must ensure policy.Validate() == nil
func NewPlanner(secsPerState *prometheus.HistogramVec, bytesReplicated *prometheus.CounterVec, sender Sender, receiver Receiver, policy PlannerPolicy) *Planner {
	if err := policy.Validate(); err != nil {
		panic(err)
	}
	return &Planner{
		sender:              sender,
		receiver:            receiver,
		policy:              policy,
		promSecsPerState:    secsPerState,
		promBytesReplicated: bytesReplicated,
	}
}

func tryAutoresolveConflict(conflict error, policy ConflictResolution) (path []*pdu.FilesystemVersion, reason error) {
	var errMostRecent *ConflictMostRecentSnapshotAlreadyPresent
	if errors.As(conflict, &errMostRecent) {
		// replicatoin is a no-op
		return nil, nil
	}

	var noCommonAncestor *ConflictNoCommonAncestor
	if errors.As(conflict, &noCommonAncestor) {
		if len(noCommonAncestor.SortedReceiverVersions) == 0 {

			if len(noCommonAncestor.SortedSenderVersions) == 0 {
				return nil, errors.New("no snapshots available on sender side")
			}

			switch policy.InitialReplication {

			case InitialReplicationAutoResolutionMostRecent:

				var mostRecentSnap *pdu.FilesystemVersion
				for n := len(noCommonAncestor.SortedSenderVersions) - 1; n >= 0; n-- {
					if noCommonAncestor.SortedSenderVersions[n].Type == pdu.FilesystemVersion_Snapshot {
						mostRecentSnap = noCommonAncestor.SortedSenderVersions[n]
						break
					}
				}
				return []*pdu.FilesystemVersion{nil, mostRecentSnap}, nil

			case InitialReplicationAutoResolutionAll:

				path = append(path, nil)

				for n := 0; n < len(noCommonAncestor.SortedSenderVersions); n++ {
					if noCommonAncestor.SortedSenderVersions[n].Type == pdu.FilesystemVersion_Snapshot {
						path = append(path, noCommonAncestor.SortedSenderVersions[n])
					}
				}
				return path, nil

			case InitialReplicationAutoResolutionFail:

				return nil, errors.New("automatic conflict resolution for initial replication is disabled in config")

			default:
				panic(fmt.Sprintf("unimplemented: %#v", policy.InitialReplication))
			}
		}
	}
	return nil, conflict
}

func (p *Planner) doPlanning(ctx context.Context) ([]*Filesystem, error) {
	log := getLogger(ctx)

	log.Info("start planning")
	var sfss, rfss []*pdu.Filesystem
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		slfssres, err := p.sender.ListFilesystems(ctx)
		if err != nil {
			logger.WithError(
				log.With(slog.String("errType", fmt.Sprintf("%T", err))),
				err, "error listing sender filesystems")
			return err
		}
		sfss = slfssres.GetFilesystems()
		return nil
	})

	g.Go(func() error {
		rlfssres, err := p.receiver.ListFilesystems(ctx)
		if err != nil {
			logger.WithError(
				log.With(slog.String("errType", fmt.Sprintf("%T", err))),
				err, "error listing receiver filesystems")
			return err
		}
		rfss = rlfssres.GetFilesystems()
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, err //nolint:wrapcheck // out error
	}

	q := make([]*Filesystem, 0, len(sfss))
	for _, fs := range sfss {
		var receiverFS *pdu.Filesystem
		for _, rfs := range rfss {
			if rfs.Path == fs.Path {
				receiverFS = rfs
			}
		}

		var ctr prometheus.Counter
		if p.promBytesReplicated != nil {
			ctr = p.promBytesReplicated.WithLabelValues(fs.Path)
		}

		q = append(q, &Filesystem{
			sender:              p.sender,
			receiver:            p.receiver,
			policy:              p.policy,
			Path:                fs.Path,
			senderFS:            fs,
			receiverFS:          receiverFS,
			promBytesReplicated: ctr,
		})
	}
	return q, nil
}

func (fs *Filesystem) doPlanning(ctx context.Context, prefix string) ([]*Step,
	error,
) {
	log := getLogger(ctx).With(slog.String("filesystem", fs.Path))
	if prefix != "" {
		log.With(slog.String("prefix", prefix))
	}
	log.Debug("assessing filesystem")

	if fs.senderFS.IsPlaceholder {
		log.Debug("sender filesystem is placeholder")
		if fs.receiverFS != nil {
			if !fs.receiverFS.IsPlaceholder {
				err := errors.New(
					"sender filesystem is placeholder, but receiver filesystem is not")
				log.Error(err.Error())
				return nil, err
			}
			// all good, fall through
			log.Debug("receiver filesystem is placeholder")
		}
		log.Debug("no steps required for replicating placeholders, the endpoint.Receiver will create a placeholder when we receive the first non-placeholder child filesystem")
		return nil, nil
	}

	fsvsResps, err := fs.listBothVersions(ctx)
	if err != nil {
		logger.WithError(log, err,
			"cannot get sender/receiver filesystem versions")
		return nil, err
	}
	sfsvs := fsvsResps[0].GetVersions()
	if len(sfsvs) < 1 {
		err := errors.New("sender does not have any versions")
		log.Error(err.Error())
		return nil, err
	}

	var rfsvs []*pdu.FilesystemVersion
	if fs.needReceiverVersions() {
		rfsvs = fsvsResps[1].GetVersions()
	} else {
		rfsvs = []*pdu.FilesystemVersion{}
	}

	var resumeToken *zfs.ResumeToken
	var resumeTokenRaw string
	if fs.receiverFS != nil && fs.receiverFS.ResumeToken != "" {
		resumeTokenRaw = fs.receiverFS.ResumeToken // shadow
		log.With(slog.String("receiverFS.ResumeToken", resumeTokenRaw)).
			Debug("decode receiver fs resume token")
		resumeToken, err = zfs.ParseResumeToken(ctx, resumeTokenRaw) // shadow
		if err != nil {
			// TODO in theory, we could do replication without resume token, but that
			// would mean that we need to discard the resumable state on the
			// receiver's side. Would be easy by setting UsedResumeToken=false in the
			// RecvReq ...
			//
			// FIXME / CHECK semantics UsedResumeToken if SendReq.ResumeToken == ""
			logger.WithError(log, err, "cannot decode resume token, aborting")
			return nil, err
		}
		log.With(slog.Any("token", resumeToken)).Debug("decode resume token")
	}

	// build the list of replication steps
	//
	// prefer to resume any started replication instead of starting over with a
	// normal IncrementalPath
	//
	// look for the step encoded in the resume token in the sender's version
	// if we find that step:
	//
	//   1. use it as first step (including resume token)
	//
	//   2. compute subsequent steps by computing incremental path from the
	//      token.To version on
	//
	//   ...
	//
	//   that's actually equivalent to simply cutting off earlier versions from
	//   rfsvs and sfsvs
	var steps []*Step
	if resumeToken != nil {
		sfsvs := SortVersionListByCreateTXGThenBookmarkLTSnapshot(sfsvs)

		var from, to *pdu.FilesystemVersion
		for s := range slices.Values(sfsvs) {
			if resumeToken.HasFromGUID && s.Guid == resumeToken.FromGUID {
				if from != nil && from.Type == pdu.FilesystemVersion_Snapshot {
					// prefer snapshots over bookmarks for size estimation
				} else {
					from = s
				}
			}
			if resumeToken.HasToGUID && s.Guid == resumeToken.ToGUID &&
				s.Type == pdu.FilesystemVersion_Snapshot {
				// `to` must always be a snapshot
				to = s
			}
		}

		if to == nil {
			return nil, fmt.Errorf(
				"resume token `toguid` = %v not found on sender (`toname` = %q)",
				resumeToken.ToGUID, resumeToken.ToName)
		} else if from == to {
			return nil, errors.New(
				"resume token `fromguid` and `toguid` match same version on sener")
		}

		// `from` may be nil, `to` is no nil, encryption matches good to go this one
		// step!
		resumeStep := NewStep(fs, from, to)
		resumeStep.resumeToken = resumeTokenRaw

		// By definition, the resume token _must_ be the receiver's most recent
		// version, if they have any don't bother checking, zfs recv will produce an
		// error if above assumption is wrong thus, subsequent steps are just
		// incrementals on the sender's remaining _snapshots_ (not bookmarks)
		steps = makeSteps(fs, prefix, resumeStep, slices.DeleteFunc(sfsvs,
			func(s *pdu.FilesystemVersion) bool {
				return s.Type != pdu.FilesystemVersion_Snapshot
			}))
	} else { // resumeToken == nil
		path, conflict := IncrementalPath(rfsvs, sfsvs)
		if conflict != nil {
			updPath, updConflict := tryAutoresolveConflict(conflict,
				*fs.policy.ConflictResolution)
			if updConflict != nil {
				log.With(slog.String("conflict", conflict.Error())).
					Error("cannot resolve conflict")
				return nil, updConflict
			}
			log.With(slog.String("conflict", conflict.Error())).
				Info("conflict automatically resolved")
			path = updPath
		}

		switch {
		case len(path) == 1:
			panic(fmt.Sprintf(
				"len(path) must be two for incremental repl, and initial repl must start with nil, got path[0]=%#v",
				path[0]))
		case len(path) > 1:
			steps = makeSteps(fs, prefix, nil, path)
		}
	}

	if len(steps) == 0 {
		log.Info("planning determined that no replication steps are required")
		return steps, nil
	}
	log.With(slog.Int("steps", len(steps))).Debug(
		"planning determined replication required")

	log.Debug("compute send size estimate")
	if err := fs.updateSizeEstimates(ctx, steps); err != nil {
		logger.WithError(log, err, "error computing size estimate")
	}
	log.Debug("filesystem planning finished")
	return steps, nil
}

func (fs *Filesystem) listBothVersions(ctx context.Context,
) (resps [2]*pdu.ListFilesystemVersionsRes, err error) {
	req := pdu.ListFilesystemVersionsReq{Filesystem: fs.Path}
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		resp, err := fs.sender.ListFilesystemVersions(ctx, &req)
		if err != nil {
			return fmt.Errorf("sender: %w", err)
		}
		resps[0] = resp
		return nil
	})

	if fs.needReceiverVersions() {
		g.Go(func() error {
			resp, err := fs.receiver.ListFilesystemVersions(ctx, &req)
			if err != nil {
				return fmt.Errorf("receiver: %w", err)
			}
			resps[1] = resp
			return nil
		})
	}
	return resps, g.Wait() //nolint:wrapcheck // our error
}

func (fs *Filesystem) needReceiverVersions() bool {
	return fs.receiverFS != nil && !fs.receiverFS.GetIsPlaceholder()
}

func (fs *Filesystem) updateSizeEstimates(ctx context.Context, steps []*Step,
) error {
	req := pdu.SendDryReq{
		Items:       make([]pdu.SendReq, len(steps)),
		Concurrency: fs.policy.SizeEstimationConcurrency,
	}
	for i, s := range steps {
		req.Items[i] = s.buildSendRequest()
	}

	log := getLogger(ctx)
	log.Debug("initiate dry run send request")
	resp, err := fs.sender.SendDry(ctx, &req)
	if err != nil {
		logger.WithError(log, err, "dry run send request failed")
		return err
	}

	for i := range resp.Items {
		steps[i].expectedSize = resp.Items[i].GetExpectedSize()
	}
	return nil
}
