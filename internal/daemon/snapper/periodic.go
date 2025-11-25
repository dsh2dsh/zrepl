package snapper

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/dsh2dsh/cron/v3"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/config/env"
	"github.com/dsh2dsh/zrepl/internal/daemon/filters"
	"github.com/dsh2dsh/zrepl/internal/daemon/hooks"
	"github.com/dsh2dsh/zrepl/internal/daemon/job/signal"
	"github.com/dsh2dsh/zrepl/internal/daemon/logging"
	"github.com/dsh2dsh/zrepl/internal/daemon/nanosleep"
	"github.com/dsh2dsh/zrepl/internal/logger"
	"github.com/dsh2dsh/zrepl/internal/zfs"
)

func periodicFromConfig(fsf *filters.DatasetFilter,
	in *config.SnapshottingPeriodic,
) (*Periodic, error) {
	if in.Prefix == "" {
		return nil, errors.New("prefix must not be empty")
	}

	cronSpec := in.CronSpec()
	if !in.Interval.Manual {
		switch cronSpec {
		case "":
			return nil, errors.New("both interval and cron not configured")
		default:
			if _, err := cron.ParseStandard(cronSpec); err != nil {
				return nil, fmt.Errorf("parse cron spec %q: %w", cronSpec, err)
			}
		}
	}

	hookList, err := hooks.ListFromConfig(in.Hooks)
	if err != nil {
		return nil, fmt.Errorf("hook config error: %w", err)
	}

	concurrency := int(in.Concurrency)
	if concurrency < 1 {
		concurrency = runtime.GOMAXPROCS(0)
	}

	s := &Periodic{
		cronSpec: cronSpec,
		args: periodicArgs{
			interval: in.Interval.Duration().Truncate(time.Second),
			fsf:      fsf,
			planArgs: planArgs{
				prefix:          in.Prefix,
				timestampFormat: in.TimestampFormat,
				timestampLocal:  in.TimestampLocal,
				hooks:           hookList,
				concurrency:     concurrency,
			},
			writtenThreshold: in.WrittenThreshold,
			// ctx and log is set in Run()
		},

		state:     Stopped,
		nextState: Planning,
	}
	return s.init(), nil
}

type periodicArgs struct {
	ctx              context.Context
	interval         time.Duration
	fsf              *filters.DatasetFilter
	planArgs         planArgs
	writtenThreshold uint64
}

type Periodic struct {
	cronSpec string
	args     periodicArgs

	mu        sync.Mutex
	state     State
	nextState State

	// set in state Plan, used in Waiting
	lastInvocation time.Time

	// valid for state Snapshotting
	plan *plan

	// valid for state SyncUp and Waiting
	sleepUntil time.Time

	// valid for state Err
	err error
}

var _ Snapper = (*Periodic)(nil)

func (self *Periodic) init() *Periodic {
	if self.args.interval > 0 {
		self.nextState = SyncUp
	}
	return self
}

func (self *Periodic) Cron() string { return self.cronSpec }

func (self *Periodic) Periodic() bool { return true }

func (self *Periodic) Runnable() bool { return self.nextState == SyncUp }

func (self *Periodic) Run(ctx context.Context) {
	log := getLogger(ctx).With(slog.String("snapper", "periodic"))
	state, err := self.switchState()
	if err != nil {
		logger.WithError(log.With(slog.String("state", state.String())),
			err, "failed start snapper")
	}

	log.With(slog.Int("concurrency", self.args.planArgs.concurrency)).
		Info("start snapper")
	defer log.Info("exiting snapper")

	u := func(u func(*Periodic)) State {
		self.mu.Lock()
		defer self.mu.Unlock()
		if u != nil {
			u(self)
		}
		return self.state
	}
	self.args.ctx = ctx

	for st := state.sf(); st != nil; {
		preState := u(nil)
		st = st(self.args, u)
		nextState := u(nil)
		log.With(slog.String("from", preState.String()),
			slog.String("to", nextState.String()),
		).Info("state transition")
	}
}

func (self *Periodic) switchState() (State, error) {
	self.mu.Lock()
	defer self.mu.Unlock()
	if self.state.Running() {
		return self.state, fmt.Errorf("unexpected state: %s", self.state.String())
	}

	switch self.nextState {
	case SyncUp:
		self.state, self.nextState = self.nextState, Planning
	case Planning:
		self.state = self.nextState
	default:
		return self.state, fmt.Errorf("unexpected next state: %s",
			self.nextState.String())
	}
	return self.state, nil
}

func onErr(err error, u updater) state {
	return u(func(self *Periodic) {
		self.err = err
		preState := self.state
		switch self.state {
		case SyncUp:
			self.state = SyncUpErrWait
		case Planning, Snapshotting:
			self.state = ErrorWait
		}
		log := getLogger(self.args.ctx).With(
			slog.String("pre_state", preState.String()),
			slog.String("post_state", self.state.String()))
		logger.WithError(log, err, "snapshotting error")
	}).sf()
}

func onMainCtxDone(ctx context.Context, u updater) state {
	return u(func(self *Periodic) {
		self.err = context.Cause(ctx)
		self.state = Stopped
	}).sf()
}

func periodicStateSyncUp(a periodicArgs, u updater) state {
	u(func(self *Periodic) { self.lastInvocation = time.Now() })

	fss, err := zfs.ZFSListMapping(a.ctx, a.fsf)
	if err != nil {
		return onErr(err, u)
	}

	syncPoint, err := findSyncPoint(a.ctx, fss, a.planArgs.prefix, a.interval)
	if err != nil {
		return onErr(err, u)
	}

	if syncPoint.After(time.Now()) {
		u(func(self *Periodic) { self.sleepUntil = syncPoint })
		getLogger(a.ctx).With(
			slog.Duration("duration",
				time.Until(syncPoint).Truncate(time.Second)),
			slog.Time("time", syncPoint),
		).Info("waiting for sync point")
		t := nanosleep.NewTimer(time.Until(syncPoint))
		select {
		case <-t.C():
		case <-signal.WakeupFrom(a.ctx).Done():
			t.Stop()
		case <-a.ctx.Done():
			t.Stop()
		}
		u(func(self *Periodic) { self.sleepUntil = time.Time{} })
		if a.ctx.Err() != nil {
			return onMainCtxDone(a.ctx, u)
		}
	}
	return u(func(self *Periodic) { self.state = Planning }).sf()
}

func periodicStatePlan(a periodicArgs, u updater) state {
	u(func(self *Periodic) { self.lastInvocation = time.Now() })

	fss, err := zfs.ZFSListMapping(a.ctx, a.fsf)
	if err != nil {
		return onErr(err, u)
	}

	if a.writtenThreshold != 0 {
		log := getLogger(a.ctx)
		fss = slices.DeleteFunc(fss, func(p *zfs.DatasetPath) bool {
			if p.Written() < a.writtenThreshold {
				log.Info("skip snapshotting, because 'written' below threshold",
					slog.String("fs", p.ToString()),
					slog.Uint64("written", p.Written()),
					slog.Uint64("threshold", a.writtenThreshold))
				return true
			}
			return false
		})
	}
	p := makePlan(a.planArgs, fss)

	return u(func(self *Periodic) {
		self.state = Snapshotting
		self.plan = p
		self.err = nil
	}).sf()
}

func periodicStateSnapshot(a periodicArgs, u updater) state {
	var plan *plan
	u(func(self *Periodic) { plan = self.plan })

	if !plan.execute(a.ctx, false) {
		return u(func(self *Periodic) {
			self.state = ErrorWait
			self.err = errors.New(
				"one or more snapshots could not be created, check logs for details")
		}).sf()
	}

	return u(func(self *Periodic) {
		self.state = Stopped
		self.err = nil
	}).sf()
}

// see docs/snapshotting.rst
func findSyncPoint(ctx context.Context, fss []*zfs.DatasetPath, prefix string,
	interval time.Duration,
) (syncPoint time.Time, err error) {
	const (
		prioHasVersions int = iota
		prioNoVersions
	)

	type snapTime struct {
		ds   *zfs.DatasetPath
		prio int // lower is higher
		time time.Time
	}

	if len(fss) == 0 {
		return time.Now(), nil
	}

	snaptimes := make([]snapTime, 0, len(fss))
	hardErrs := 0
	now := time.Now()

	getLogger(ctx).Debug("examine filesystem state to find sync point")
	for _, d := range fss {
		ctx := logging.With(ctx, slog.String("fs", d.ToString()))
		syncPoint, err := findSyncPointFSNextOptimalSnapshotTime(
			ctx, now, interval, prefix, d)
		switch {
		case errors.Is(err, findSyncPointFSNoFilesystemVersionsErr):
			snaptimes = append(snaptimes, snapTime{
				ds:   d,
				prio: prioNoVersions,
				time: now,
			})
		case err != nil:
			hardErrs++
			logger.WithError(getLogger(ctx), err,
				"cannot determine optimal sync point for this filesystem")
		default:
			getLogger(ctx).With(slog.Time("syncPoint", syncPoint)).Debug(
				"found optimal sync point for this filesystem")
			snaptimes = append(snaptimes, snapTime{
				ds:   d,
				prio: prioHasVersions,
				time: syncPoint,
			})
		}
	}

	if hardErrs == len(fss) {
		return time.Time{}, errors.New(
			"hard errors in determining sync point for every matching filesystem")
	}

	if len(snaptimes) == 0 {
		panic("implementation error: loop must either inc hardErrs or add result to snaptimes")
	}

	// sort ascending by (prio,time)
	// => those filesystems with versions win over those without any
	sort.Slice(snaptimes, func(i, j int) bool {
		if snaptimes[i].prio == snaptimes[j].prio {
			return snaptimes[i].time.Before(snaptimes[j].time)
		}
		return snaptimes[i].prio < snaptimes[j].prio
	})

	winnerSyncPoint := snaptimes[0].time
	l := getLogger(ctx).With(slog.String("syncPoint", winnerSyncPoint.String()))
	l.Info("determined sync point")
	if winnerSyncPoint.Sub(now) > env.Values.SnapperSyncUpWarnMin {
		for _, st := range snaptimes {
			if st.prio == prioNoVersions {
				l.With(slog.String("fs", st.ds.ToString())).Warn(
					"filesystem will not be snapshotted until sync point")
			}
		}
	}
	return winnerSyncPoint, nil
}

var findSyncPointFSNoFilesystemVersionsErr = errors.New(
	"no filesystem versions")

func findSyncPointFSNextOptimalSnapshotTime(ctx context.Context, now time.Time,
	interval time.Duration, prefix string, d *zfs.DatasetPath,
) (time.Time, error) {
	fsvs, err := zfs.ZFSListFilesystemVersions(ctx, d,
		zfs.ListFilesystemVersionsOptions{
			Types:           zfs.Snapshots,
			ShortnamePrefix: prefix,
		})
	if err != nil {
		return time.Time{}, fmt.Errorf("list filesystem versions: %w", err)
	} else if len(fsvs) == 0 {
		return time.Time{}, findSyncPointFSNoFilesystemVersionsErr
	}

	// Sort versions by creation
	sort.SliceStable(fsvs, func(i, j int) bool {
		return fsvs[i].CreateTXG < fsvs[j].CreateTXG
	})

	latest := fsvs[len(fsvs)-1]
	getLogger(ctx).With(slog.Time("creation", latest.Creation)).Debug(
		"found latest snapshot")

	since := now.Sub(latest.Creation)
	if since < 0 {
		return time.Time{}, fmt.Errorf(
			"snapshot %q is from the future: creation=%q now=%q",
			latest.ToAbsPath(d), latest.Creation, now)
	}
	return latest.Creation.Add(interval), nil
}

func (self *Periodic) Running() (d time.Duration, ok bool) {
	self.mu.Lock()
	defer self.mu.Unlock()
	if !self.lastInvocation.IsZero() {
		d = time.Since(self.lastInvocation)
	}
	switch self.state {
	case Planning, Snapshotting:
		ok = true
	}
	return d, ok
}

func (self *Periodic) Report() Report {
	self.mu.Lock()
	defer self.mu.Unlock()

	var progress []*ReportFilesystem = nil
	if self.plan != nil {
		progress = self.plan.report()
	}

	r := &PeriodicReport{
		CronSpec:   self.cronSpec,
		State:      self.state,
		SleepUntil: self.sleepUntil,
		Error:      errOrEmptyString(self.err),
		Progress:   progress,
		StartedAt:  self.lastInvocation,
	}
	return Report{Type: TypePeriodic, Periodic: r}
}

type PeriodicReport struct {
	CronSpec string
	State    State
	// valid in states Planning and Snapshotting
	StartedAt time.Time
	// valid in state SyncUp and Waiting
	SleepUntil time.Time
	// valid in state Err
	Error string
	// valid in state Snapshotting
	Progress []*ReportFilesystem
}

func (self *PeriodicReport) Running() (d time.Duration, ok bool) {
	if !self.StartedAt.IsZero() {
		d = time.Since(self.StartedAt)
	}
	switch self.State {
	case Planning, Snapshotting:
		ok = true
	}
	return d, ok
}

func (self *PeriodicReport) SortProgress() []*ReportFilesystem {
	slices.SortFunc(self.Progress, func(a, b *ReportFilesystem) int {
		return cmp.Compare(a.Path, b.Path)
	})
	return self.Progress
}

func (self *PeriodicReport) IsTerminal() bool { return self.State.IsTerminal() }

func (self *PeriodicReport) CompletionProgress() (expected, completed uint64) {
	for _, fs := range self.Progress {
		expected++
		switch fs.State {
		case SnapDone, SnapError:
			completed++
		}
	}
	return expected, completed
}
