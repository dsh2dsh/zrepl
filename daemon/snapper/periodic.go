package snapper

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/zrepl/zrepl/config"
	"github.com/zrepl/zrepl/daemon/hooks"
	"github.com/zrepl/zrepl/daemon/logging"
	"github.com/zrepl/zrepl/daemon/logging/trace"
	"github.com/zrepl/zrepl/util/envconst"
	"github.com/zrepl/zrepl/zfs"
)

func periodicFromConfig(fsf zfs.DatasetFilter, in *config.SnapshottingPeriodic,
) (*Periodic, error) {
	if in.Prefix == "" {
		return nil, errors.New("prefix must not be empty")
	}

	cronSpec := in.CronSpec()
	if cronSpec == "" {
		return nil, errors.New("both interval and cron not configured")
	}

	d := in.Interval.Duration()
	if d < 0 {
		return nil, errors.New("negative interval")
	} else if _, err := cron.ParseStandard(cronSpec); err != nil {
		return nil, fmt.Errorf("parse cron spec %q: %w", cronSpec, err)
	}

	hookList, err := hooks.ListFromConfig(&in.Hooks)
	if err != nil {
		return nil, fmt.Errorf("hook config error: %w", err)
	}

	args := periodicArgs{
		interval: d.Truncate(time.Second),
		fsf:      fsf,
		planArgs: planArgs{
			prefix:          in.Prefix,
			timestampFormat: in.TimestampFormat,
			hooks:           hookList,
		},
		cronSpec: cronSpec,
		// ctx and log is set in Run()
	}

	return &Periodic{state: SyncUp, args: args}, nil
}

type periodicArgs struct {
	ctx            context.Context
	interval       time.Duration
	fsf            zfs.DatasetFilter
	planArgs       planArgs
	snapshotsTaken chan<- struct{}
	dryRun         bool

	cron     *cron.Cron
	cronSpec string
	wakeup   chan struct{}
}

type Periodic struct {
	args periodicArgs

	mtx   sync.Mutex
	state State

	// set in state Plan, used in Waiting
	lastInvocation time.Time

	// valid for state Snapshotting
	plan *plan

	// valid for state SyncUp and Waiting
	sleepUntil time.Time
	cronId     cron.EntryID

	// valid for state Err
	err        error
	wakeupBusy int
}

//go:generate stringer -type=State
type State uint

const (
	SyncUp State = 1 << iota
	SyncUpErrWait
	Planning
	Snapshotting
	Waiting
	ErrorWait
	Stopped
)

func (s State) sf() state {
	m := map[State]state{
		SyncUp:        periodicStateSyncUp,
		SyncUpErrWait: periodicStateWait,
		Planning:      periodicStatePlan,
		Snapshotting:  periodicStateSnapshot,
		Waiting:       periodicStateWait,
		ErrorWait:     periodicStateWait,
		Stopped:       nil,
	}
	return m[s]
}

type (
	updater func(u func(*Periodic)) State
	state   func(a periodicArgs, u updater) state
)

func (s *Periodic) Run(ctx context.Context, snapshotsTaken chan<- struct{},
	cron *cron.Cron,
) {
	defer trace.WithSpanFromStackUpdateCtx(&ctx)()
	getLogger(ctx).Debug("start")
	defer getLogger(ctx).Debug("stop")

	s.args.snapshotsTaken = snapshotsTaken
	s.args.ctx = ctx
	s.args.dryRun = false // for future expansion
	s.args.cron = cron
	s.args.wakeup = make(chan struct{})

	u := func(u func(*Periodic)) State {
		s.mtx.Lock()
		defer s.mtx.Unlock()
		if u != nil {
			u(s)
		}
		return s.state
	}

	st := periodicStateSyncUp
	if s.args.interval == 0 {
		st = periodicStateWait
		s.runPeriodic(ctx)
	}

	for st != nil {
		pre := u(nil)
		st = st(s.args, u)
		post := u(nil)
		getLogger(ctx).
			WithField("transition", fmt.Sprintf("%s=>%s", pre, post)).
			Debug("state transition")

	}
}

func (s *Periodic) runPeriodic(ctx context.Context) {
	cronSpec := s.args.cronSpec
	log := getLogger(ctx).WithField("cron", cronSpec)
	id, err := s.args.cron.AddFunc(cronSpec, func() {
		select {
		case s.args.wakeup <- struct{}{}:
			s.wakeupBusy = 0
		case <-ctx.Done():
		default:
			s.wakeupBusy++
			log.WithField("cron", cronSpec).Warn(
				"job took longer than its interval")
		}
	})
	if err != nil {
		log.WithError(err).Error("add cron job")
		return
	}
	s.cronId = id
	log.WithField("id", id).Info("add cron job")
}

func onErr(err error, u updater) state {
	return u(func(s *Periodic) {
		s.err = err
		preState := s.state
		switch s.state {
		case SyncUp:
			s.state = SyncUpErrWait
		case Planning:
			fallthrough
		case Snapshotting:
			s.state = ErrorWait
		}
		getLogger(s.args.ctx).WithError(err).WithField("pre_state", preState).WithField("post_state", s.state).Error("snapshotting error")
	}).sf()
}

func onMainCtxDone(ctx context.Context, u updater) state {
	return u(func(s *Periodic) {
		s.err = ctx.Err()
		s.state = Stopped
	}).sf()
}

func periodicStateSyncUp(a periodicArgs, u updater) state {
	u(func(snapper *Periodic) {
		snapper.lastInvocation = time.Now()
	})
	fss, err := listFSes(a.ctx, a.fsf)
	if err != nil {
		return onErr(err, u)
	}
	syncPoint, err := findSyncPoint(a.ctx, fss, a.planArgs.prefix, a.interval)
	if err != nil {
		return onErr(err, u)
	}
	u(func(s *Periodic) {
		s.sleepUntil = syncPoint
	})

	nextState := Planning
	if syncPoint.After(time.Now()) {
		t := time.NewTimer(time.Until(syncPoint))
		select {
		case <-t.C:
		case <-a.ctx.Done():
			t.Stop()
		}
		if a.ctx.Err() != nil {
			return onMainCtxDone(a.ctx, u)
		}
	} else {
		nextState = Waiting
	}

	return u(func(s *Periodic) {
		s.state = nextState
		s.runPeriodic(a.ctx)
	}).sf()
}

func periodicStatePlan(a periodicArgs, u updater) state {
	u(func(snapper *Periodic) {
		snapper.lastInvocation = time.Now()
	})
	fss, err := listFSes(a.ctx, a.fsf)
	if err != nil {
		return onErr(err, u)
	}
	p := makePlan(a.planArgs, fss)
	return u(func(s *Periodic) {
		s.state = Snapshotting
		s.plan = p
		s.err = nil
	}).sf()
}

func periodicStateSnapshot(a periodicArgs, u updater) state {
	var plan *plan
	u(func(snapper *Periodic) {
		plan = snapper.plan
	})

	ok := plan.execute(a.ctx, false)

	select {
	case a.snapshotsTaken <- struct{}{}:
	default:
		if a.snapshotsTaken != nil {
			getLogger(a.ctx).Warn("callback channel is full, discarding snapshot update event")
		}
	}

	return u(func(snapper *Periodic) {
		if !ok {
			snapper.state = ErrorWait
			snapper.err = errors.New("one or more snapshots could not be created, check logs for details")
		} else {
			snapper.state = Waiting
			snapper.err = nil
		}
	}).sf()
}

func periodicStateWait(a periodicArgs, u updater) state {
	u(func(snapper *Periodic) {
		log := getLogger(a.ctx)
		logFunc := log.Debug
		if snapper.state == ErrorWait || snapper.state == SyncUpErrWait {
			logFunc = log.Error
		}
		logFunc("enter wait-state after error")
	})

	select {
	case <-a.wakeup:
	case <-a.ctx.Done():
	}
	if a.ctx.Err() != nil {
		return onMainCtxDone(a.ctx, u)
	}
	return u(func(snapper *Periodic) { snapper.state = Planning }).sf()
}

func listFSes(ctx context.Context, mf zfs.DatasetFilter) (fss []*zfs.DatasetPath, err error) {
	return zfs.ZFSListMapping(ctx, mf)
}

var syncUpWarnNoSnapshotUntilSyncupMinDuration = envconst.Duration("ZREPL_SNAPPER_SYNCUP_WARN_MIN_DURATION", 1*time.Second)

// see docs/snapshotting.rst
func findSyncPoint(ctx context.Context, fss []*zfs.DatasetPath, prefix string, interval time.Duration) (syncPoint time.Time, err error) {
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
		ctx := logging.WithInjectedField(ctx, "fs", d.ToString())
		syncPoint, err := findSyncPointFSNextOptimalSnapshotTime(ctx, now, interval, prefix, d)
		if err == findSyncPointFSNoFilesystemVersionsErr {
			snaptimes = append(snaptimes, snapTime{
				ds:   d,
				prio: prioNoVersions,
				time: now,
			})
		} else if err != nil {
			hardErrs++
			getLogger(ctx).WithError(err).Error("cannot determine optimal sync point for this filesystem")
		} else {
			getLogger(ctx).WithField("syncPoint", syncPoint).Debug("found optimal sync point for this filesystem")
			snaptimes = append(snaptimes, snapTime{
				ds:   d,
				prio: prioHasVersions,
				time: syncPoint,
			})
		}
	}

	if hardErrs == len(fss) {
		return time.Time{}, fmt.Errorf("hard errors in determining sync point for every matching filesystem")
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
	l := getLogger(ctx).WithField("syncPoint", winnerSyncPoint.String())
	l.Info("determined sync point")
	if winnerSyncPoint.Sub(now) > syncUpWarnNoSnapshotUntilSyncupMinDuration {
		for _, st := range snaptimes {
			if st.prio == prioNoVersions {
				l.WithField("fs", st.ds.ToString()).Warn("filesystem will not be snapshotted until sync point")
			}
		}
	}

	return snaptimes[0].time, nil
}

var findSyncPointFSNoFilesystemVersionsErr = fmt.Errorf("no filesystem versions")

func findSyncPointFSNextOptimalSnapshotTime(ctx context.Context, now time.Time, interval time.Duration, prefix string, d *zfs.DatasetPath) (time.Time, error) {
	fsvs, err := zfs.ZFSListFilesystemVersions(ctx, d, zfs.ListFilesystemVersionsOptions{
		Types:           zfs.Snapshots,
		ShortnamePrefix: prefix,
	})
	if err != nil {
		return time.Time{}, fmt.Errorf("list filesystem versions: %w", err)
	}
	if len(fsvs) <= 0 {
		return time.Time{}, findSyncPointFSNoFilesystemVersionsErr
	}

	// Sort versions by creation
	sort.SliceStable(fsvs, func(i, j int) bool {
		return fsvs[i].CreateTXG < fsvs[j].CreateTXG
	})

	latest := fsvs[len(fsvs)-1]
	getLogger(ctx).WithField("creation", latest.Creation).Debug("found latest snapshot")

	since := now.Sub(latest.Creation)
	if since < 0 {
		return time.Time{}, fmt.Errorf("snapshot %q is from the future: creation=%q now=%q", latest.ToAbsPath(d), latest.Creation, now)
	}

	return latest.Creation.Add(interval), nil
}

type PeriodicReport struct {
	CronSpec string
	State    State
	// valid in state SyncUp and Waiting
	SleepUntil time.Time
	// valid in state Err
	Error string
	// valid in state Snapshotting
	Progress []*ReportFilesystem
}

func (s *Periodic) Report() Report {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	var progress []*ReportFilesystem = nil
	if s.plan != nil {
		progress = s.plan.report()
	}

	sleepUntil := s.sleepUntil
	if s.cronId > 0 {
		sleepUntil = s.args.cron.Entry(s.cronId).Next
	}

	err := s.err
	if err == nil && s.wakeupBusy > 0 {
		err = fmt.Errorf(
			"job frequency is too high; snapshots were not taken %d times",
			s.wakeupBusy)
	}

	r := &PeriodicReport{
		CronSpec:   s.args.cronSpec,
		State:      s.state,
		SleepUntil: sleepUntil,
		Error:      errOrEmptyString(err),
		Progress:   progress,
	}

	return Report{Type: TypePeriodic, Periodic: r}
}
