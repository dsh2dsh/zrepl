package job

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/daemon/filters"
	"github.com/dsh2dsh/zrepl/internal/daemon/job/signal"
	"github.com/dsh2dsh/zrepl/internal/daemon/pruner"
	"github.com/dsh2dsh/zrepl/internal/daemon/snapper"
	"github.com/dsh2dsh/zrepl/internal/endpoint"
	"github.com/dsh2dsh/zrepl/internal/replication/logic/pdu"
	"github.com/dsh2dsh/zrepl/internal/zfs"
)

func snapJobFromConfig(g *config.Global, in *config.SnapJob) (j *SnapJob,
	err error,
) {
	j = &SnapJob{pruneConcurrency: int(in.Pruning.Concurrency)}
	fsf, err := filters.NewFromConfig(in.Filesystems, in.Datasets)
	if err != nil {
		return nil, fmt.Errorf("cannot build filesystem filter: %w", err)
	}
	j.fsfilter = fsf

	if j.snapper, err = snapper.FromConfig(g, fsf, in.Snapshotting); err != nil {
		return nil, fmt.Errorf("cannot build snapper: %w", err)
	}
	j.name, err = endpoint.MakeJobID(in.Name)
	if err != nil {
		return nil, fmt.Errorf("invalid job name: %w", err)
	}
	j.promPruneSecs = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   "zrepl",
		Subsystem:   "pruning",
		Name:        "time",
		Help:        "seconds spent in pruner",
		ConstLabels: prometheus.Labels{"zrepl_job": j.name.String()},
	}, []string{"prune_side"})
	j.prunerFactory, err = pruner.NewLocalPrunerFactory(
		in.Pruning, j.promPruneSecs)
	if err != nil {
		return nil, fmt.Errorf("cannot build snapjob pruning rules: %w", err)
	}
	return j, nil
}

type SnapJob struct {
	name     endpoint.JobID
	fsfilter *filters.DatasetFilter
	snapper  snapper.Snapper

	prunerFactory *pruner.LocalPrunerFactory

	promPruneSecs *prometheus.HistogramVec // labels: prune_side

	prunerMtx sync.Mutex
	pruner    *pruner.Pruner

	pruneConcurrency int
}

var _ Job = (*SnapJob)(nil)

func (j *SnapJob) Name() string { return j.name.String() }

func (j *SnapJob) Type() Type { return TypeSnap }

func (j *SnapJob) Cron() string { return j.snapper.Cron() }

func (j *SnapJob) Runnable() bool { return j.snapper.Runnable() }

func (j *SnapJob) RegisterMetrics(registerer prometheus.Registerer) {
	registerer.MustRegister(j.promPruneSecs)
}

func (j *SnapJob) Status() *Status {
	s := &SnapJobStatus{}
	t := j.Type()
	j.prunerMtx.Lock()
	if j.pruner != nil {
		s.Pruning = j.pruner.Report()
	}
	j.prunerMtx.Unlock()
	r := j.snapper.Report()
	s.Snapshotting = &r
	return &Status{Type: t, JobSpecific: s}
}

type SnapJobStatus struct {
	Pruning      *pruner.Report
	Snapshotting *snapper.Report // may be nil
}

func (self *SnapJobStatus) Error() string {
	if prun := self.Pruning; prun != nil {
		if prun.Error != "" {
			return prun.Error
		}
		for _, fs := range prun.Completed {
			if fs.SkipReason.NotSkipped() && fs.LastError != "" {
				return fs.LastError
			}
		}
	}

	if snap := self.Snapshotting; snap != nil {
		if s := snap.Error(); s != "" {
			return s
		}
	}
	return ""
}

func (self *SnapJobStatus) Running() (d time.Duration, ok bool) {
	if s := self.Snapshotting; s != nil {
		if d, ok = s.Running(); ok {
			return
		}
	}

	if p := self.Pruning; p != nil {
		if d == 0 {
			d, ok = p.Running()
		} else {
			_, ok = p.Running()
		}
	}
	return
}

func (self *SnapJobStatus) Cron() string {
	if snap := self.Snapshotting; snap != nil {
		return snap.Cron()
	}
	return ""
}

func (self *SnapJobStatus) SleepingUntil() time.Time {
	if snap := self.Snapshotting; snap != nil {
		return snap.SleepingUntil()
	}
	return time.Time{}
}

func (self *SnapJobStatus) Steps() (expected, step int) {
	expected = 2
	if s := self.Snapshotting; s == nil {
		expected--
	} else if d, ok := s.Running(); !ok && d == 0 {
		expected--
	}

	if s := self.Snapshotting; s != nil {
		if d, ok := s.Running(); ok || d > 0 {
			step++
		}
	}

	if p := self.Pruning; p != nil {
		if d, ok := p.Running(); ok || d > 0 {
			step++
		}
	}
	return
}

func (self *SnapJobStatus) Progress() (uint64, uint64) {
	if s := self.Snapshotting; s != nil {
		if _, ok := s.Running(); ok {
			return s.Progress()
		}
	}

	if p := self.Pruning; p != nil {
		if _, ok := p.Running(); ok {
			return p.Progress()
		}
	}
	return 0, 0
}

func (j *SnapJob) OwnedDatasetSubtreeRoot() (*zfs.DatasetPath, bool) {
	return nil, false
}

func (j *SnapJob) SenderConfig() *endpoint.SenderConfig { return nil }

func (j *SnapJob) Run(ctx context.Context) error {
	log := GetLogger(ctx)
	defer log.Info("job exiting")
	ctx = signal.GracefulFrom(ctx)

	j.snapper.Run(ctx)
	if ctx.Err() != nil {
		log.With(slog.String("cause", context.Cause(ctx).Error())).
			Info("context done")
		return nil
	}
	j.prune(ctx)
	return nil
}

func (j *SnapJob) prune(ctx context.Context) {
	sender := endpoint.NewSender(endpoint.SenderConfig{
		JobID: j.name,
		FSF:   j.fsfilter,
		// FIXME the following config fields are irrelevant for SnapJob
		// because the endpoint is only used as pruner.Target.
		// However, the implementation requires them to be set.
		Encrypt: true,
	}).WithPruneConcurrency(j.pruneConcurrency)

	localSender := NewLocalSender(ctx, sender)
	pruner := j.prunerFactory.BuildLocalPruner(ctx, localSender, localSender)
	j.prunerMtx.Lock()
	j.pruner = pruner
	j.prunerMtx.Unlock()

	log := GetLogger(ctx)
	log.With(slog.Int("concurrency", j.pruner.Concurrency())).
		Info("start pruning")
	j.pruner.Prune()
	log.Info("finished pruning")
}

// Adaptor that implements pruner.History around a pruner.Target.
// The ReplicationCursor method is Get-op only and always returns
// the filesystem's most recent version's GUID.
//
// TODO:
// This is a work-around for the current package daemon/pruner
// and package pruning.Snapshot limitation: they require the
// `Replicated` getter method be present, but obviously,
// a local job like SnapJob can't deliver on that.
// But the pruner.Pruner gives up on an FS if no replication
// cursor is present, which is why this pruner returns the
// most recent filesystem version.
func NewLocalSender(ctx context.Context, target *endpoint.Sender) *LocalSender {
	return &LocalSender{NewSenderOnce(ctx, target)}
}

type LocalSender struct {
	// the Target passed as Target to BuildLocalPruner
	pruner.Target
}

var (
	_ pruner.Sender = (*LocalSender)(nil)
	_ pruner.Target = (*LocalSender)(nil)
)

func (self *LocalSender) ReplicationCursor(ctx context.Context,
	req *pdu.ReplicationCursorReq,
) (*pdu.ReplicationCursorRes, error) {
	fsvReq := &pdu.ListFilesystemVersionsReq{Filesystem: req.Filesystem}
	res, err := self.ListFilesystemVersions(ctx, fsvReq)
	if err != nil {
		return nil, err
	}

	fsvs := res.Versions
	if len(fsvs) == 0 {
		return &pdu.ReplicationCursorRes{
			Result: &pdu.ReplicationCursorRes_Result{Notexist: true},
		}, nil
	}

	// always return must recent version
	mostRecent := slices.MaxFunc(fsvs, func(a, b *pdu.FilesystemVersion) int {
		return cmp.Compare(a.CreateTXG, b.CreateTXG)
	})

	return &pdu.ReplicationCursorRes{
		Result: &pdu.ReplicationCursorRes_Result{Guid: mostRecent.GetGuid()},
	}, nil
}
