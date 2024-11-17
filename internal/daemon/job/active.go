package job

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dsh2dsh/cron/v3"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/semaphore"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/daemon/job/reset"
	"github.com/dsh2dsh/zrepl/internal/daemon/job/wakeup"
	"github.com/dsh2dsh/zrepl/internal/daemon/pruner"
	"github.com/dsh2dsh/zrepl/internal/daemon/snapper"
	"github.com/dsh2dsh/zrepl/internal/endpoint"
	"github.com/dsh2dsh/zrepl/internal/logger"
	"github.com/dsh2dsh/zrepl/internal/replication"
	"github.com/dsh2dsh/zrepl/internal/replication/driver"
	"github.com/dsh2dsh/zrepl/internal/replication/logic"
	"github.com/dsh2dsh/zrepl/internal/replication/report"
	"github.com/dsh2dsh/zrepl/internal/util/envconst"
	"github.com/dsh2dsh/zrepl/internal/zfs"
)

type ActiveSide struct {
	mode      activeMode
	name      endpoint.JobID
	connected Connected

	replicationDriverConfig driver.Config

	prunerFactory *pruner.PrunerFactory

	promRepStateSecs      *prometheus.HistogramVec // labels: state
	promPruneSecs         *prometheus.HistogramVec // labels: prune_side
	promBytesReplicated   *prometheus.CounterVec   // labels: filesystem
	promReplicationErrors prometheus.Gauge
	promLastSuccessful    prometheus.Gauge

	tasksMtx sync.Mutex
	tasks    activeSideTasks

	cron       *cron.Cron
	cronId     cron.EntryID
	wakeupBusy int

	running  context.Context
	shutdown context.CancelFunc

	preHook  *Hook
	postHook *Hook
}

//go:generate enumer -type=ActiveSideState
type ActiveSideState int

const (
	ActiveSideReplicating ActiveSideState = 1 << iota
	ActiveSidePruneSender
	ActiveSidePruneReceiver
	ActiveSideDone // also errors
)

type activeSideTasks struct {
	state     ActiveSideState
	startedAt time.Time
	err       error

	// valid for state ActiveSideReplicating, ActiveSidePruneSender,
	// ActiveSidePruneReceiver, ActiveSideDone
	replicationReport driver.ReportFunc

	// valid for state ActiveSidePruneSender, ActiveSidePruneReceiver, ActiveSideDone
	prunerSender, prunerReceiver *pruner.Pruner
}

func (a *ActiveSide) updateTasks(u func(*activeSideTasks)) activeSideTasks {
	a.tasksMtx.Lock()
	defer a.tasksMtx.Unlock()
	copy := a.tasks
	if u == nil {
		return copy
	}
	u(&copy)
	a.tasks = copy
	return copy
}

type activeMode interface {
	ConnectEndpoints(ctx context.Context, cn Connected)
	DisconnectEndpoints()
	SenderReceiver() (logic.Sender, logic.Receiver)
	Type() Type
	PlannerPolicy() logic.PlannerPolicy
	PeriodicSnapshots() bool
	RunSnapper(ctx context.Context, wakeUpCommon chan<- struct{},
		cron *cron.Cron,
	) <-chan struct{}
	Cron() string
	SnapperReport() *snapper.Report
	ResetConnectBackoff()
	Shutdown()
	Wait()
	Running() (time.Duration, bool)
}

func modePushFromConfig(g *config.Global, in *config.PushJob,
	jobID endpoint.JobID,
) (*modePush, error) {
	m := &modePush{}
	cronSpec := in.CronSpec()
	if _, ok := in.Snapshotting.Ret.(*config.SnapshottingManual); ok {
		if cronSpec != "" {
			if _, err := cron.ParseStandard(cronSpec); err != nil {
				return nil, fmt.Errorf("parse cron spec %q: %w", cronSpec, err)
			}
			m.cronSpec = cronSpec
		}
	} else if cronSpec != "" {
		return nil, fmt.Errorf(
			"both cron spec and periodic snapshotting defined: %q", cronSpec)
	}

	var err error
	m.senderConfig, err = buildSenderConfig(in, jobID)
	if err != nil {
		return nil, fmt.Errorf("sender config: %w", err)
	} else if m.senderConfig.Concurrency > 0 {
		m.sem = semaphore.NewWeighted(m.senderConfig.Concurrency)
	}

	replicationConfig, err := logic.ReplicationConfigFromConfig(
		&in.Replication)
	if err != nil {
		return nil, fmt.Errorf("field `replication`: %w", err)
	}

	conflictResolution, err := logic.ConflictResolutionFromConfig(
		&in.ConflictResolution)
	if err != nil {
		return nil, fmt.Errorf("field `conflict_resolution`: %w", err)
	}

	m.plannerPolicy = &logic.PlannerPolicy{
		ConflictResolution:        conflictResolution,
		ReplicationConfig:         replicationConfig,
		SizeEstimationConcurrency: in.Replication.Concurrency.SizeEstimates,
	}
	if err := m.plannerPolicy.Validate(); err != nil {
		return nil, fmt.Errorf("cannot build planner policy: %w", err)
	}

	m.snapper, err = snapper.FromConfig(g, m.senderConfig.FSF, in.Snapshotting)
	if err != nil {
		return nil, fmt.Errorf("cannot build snapper: %w", err)
	}
	return m, nil
}

type modePush struct {
	setupMtx      sync.Mutex
	sender        *endpoint.Sender
	receiver      Endpoint
	senderConfig  *endpoint.SenderConfig
	plannerPolicy *logic.PlannerPolicy
	snapper       snapper.Snapper
	cronSpec      string

	sem *semaphore.Weighted
	wg  sync.WaitGroup
}

func (m *modePush) ConnectEndpoints(ctx context.Context, cn Connected) {
	m.setupMtx.Lock()
	defer m.setupMtx.Unlock()

	if m.receiver != nil || m.sender != nil {
		panic("inconsistent use of ConnectEndpoints and DisconnectEndpoints")
	}

	GetLogger(ctx).
		WithField("mode", "push").
		WithField("to", cn.Name()).
		Info("connect to receiver")

	m.receiver = cn.Endpoint()
	m.sender = endpoint.NewSender(*m.senderConfig).WithSemaphore(m.sem)
}

func (m *modePush) DisconnectEndpoints() {
	m.setupMtx.Lock()
	defer m.setupMtx.Unlock()
	m.receiver = nil
	m.sender = nil
}

func (m *modePush) SenderReceiver() (logic.Sender, logic.Receiver) {
	m.setupMtx.Lock()
	defer m.setupMtx.Unlock()
	return m.sender, m.receiver
}

func (m *modePush) Type() Type { return TypePush }

func (m *modePush) PlannerPolicy() logic.PlannerPolicy {
	return *m.plannerPolicy
}

func (m *modePush) Cron() string { return m.cronSpec }

func (m *modePush) PeriodicSnapshots() bool { return m.snapper.Periodic() }

func (m *modePush) RunSnapper(ctx context.Context, wakeUpCommon chan<- struct{},
	cron *cron.Cron,
) <-chan struct{} {
	if !m.PeriodicSnapshots() {
		GetLogger(ctx).Info("periodic snapshotting disabled")
		return wakeup.Wait(ctx)

	}

	m.wg.Add(1)
	go func() {
		m.snapper.Run(ctx, wakeUpCommon, cron)
		m.wg.Done()
	}()
	return make(chan struct{})
}

func (m *modePush) SnapperReport() *snapper.Report {
	r := m.snapper.Report()
	return &r
}

func (m *modePush) ResetConnectBackoff() {}

func (m *modePush) Shutdown() {
	if m.snapper.Periodic() {
		m.snapper.Shutdown()
	}
}

func (m *modePush) Wait() { m.wg.Wait() }

func (m *modePush) Running() (time.Duration, bool) {
	if !m.snapper.Periodic() {
		return 0, false
	}
	return m.snapper.Running()
}

type modePull struct {
	setupMtx       sync.Mutex
	receiver       *endpoint.Receiver
	receiverConfig endpoint.ReceiverConfig
	sender         Endpoint
	plannerPolicy  *logic.PlannerPolicy
	cronSpec       string

	sem *semaphore.Weighted
}

func (m *modePull) ConnectEndpoints(ctx context.Context, cn Connected) {
	m.setupMtx.Lock()
	defer m.setupMtx.Unlock()
	if m.receiver != nil || m.sender != nil {
		panic("inconsistent use of ConnectEndpoints and DisconnectEndpoints")
	}

	GetLogger(ctx).
		WithField("mode", "pull").
		WithField("from", cn.Name()).
		Info("connect to sender")

	m.receiver = endpoint.NewReceiver(m.receiverConfig).WithSemaphore(m.sem)
	m.sender = cn.Endpoint()
}

func (m *modePull) DisconnectEndpoints() {
	m.setupMtx.Lock()
	defer m.setupMtx.Unlock()
	m.receiver = nil
	m.sender = nil
}

func (m *modePull) SenderReceiver() (logic.Sender, logic.Receiver) {
	m.setupMtx.Lock()
	defer m.setupMtx.Unlock()
	return m.sender, m.receiver
}

func (*modePull) Type() Type { return TypePull }

func (m *modePull) PlannerPolicy() logic.PlannerPolicy {
	return *m.plannerPolicy
}

func (m *modePull) Cron() string { return m.cronSpec }

func (m *modePull) PeriodicSnapshots() bool { return false }

func (m *modePull) RunSnapper(ctx context.Context, wakeUpCommon chan<- struct{},
	cron *cron.Cron,
) <-chan struct{} {
	GetLogger(ctx).Info("manual pull configured, periodic pull disabled")
	return wakeup.Wait(ctx)
}

func (m *modePull) SnapperReport() *snapper.Report { return nil }

func (m *modePull) ResetConnectBackoff() {}

func (m *modePull) Shutdown() {}

func (m *modePull) Wait() {}

func (m *modePull) Running() (time.Duration, bool) { return 0, false }

func modePullFromConfig(in *config.PullJob, jobID endpoint.JobID,
) (m *modePull, err error) {
	if in.Connect.Type == "local" || in.Connect.Server == "" {
		return nil, fmt.Errorf("pull job %q cannot use local connect", jobID)
	}

	m = &modePull{}
	cronSpec := in.CronSpec()
	if cronSpec != "" {
		if _, err := cron.ParseStandard(cronSpec); err != nil {
			return nil, fmt.Errorf("parse cron spec %q: %w", cronSpec, err)
		}
		m.cronSpec = cronSpec
	}

	replicationConfig, err := logic.ReplicationConfigFromConfig(
		&in.Replication)
	if err != nil {
		return nil, fmt.Errorf("field `replication`: %w", err)
	}

	conflictResolution, err := logic.ConflictResolutionFromConfig(
		&in.ConflictResolution)
	if err != nil {
		return nil, fmt.Errorf("field `conflict_resolution`: %w", err)
	}

	m.plannerPolicy = &logic.PlannerPolicy{
		ConflictResolution:        conflictResolution,
		ReplicationConfig:         replicationConfig,
		SizeEstimationConcurrency: in.Replication.Concurrency.SizeEstimates,
	}
	if err := m.plannerPolicy.Validate(); err != nil {
		return nil, fmt.Errorf("cannot build planner policy: %w", err)
	}

	m.receiverConfig, err = buildReceiverConfig(in, jobID)
	if err != nil {
		return nil, err
	} else if m.receiverConfig.Concurrency > 0 {
		m.sem = semaphore.NewWeighted(m.receiverConfig.Concurrency)
	}
	return m, nil
}

func replicationDriverConfigFromConfig(in *config.Replication) (driver.Config,
	error,
) {
	c := driver.Config{
		StepQueueConcurrency: in.Concurrency.Steps,
		MaxAttempts:          envconst.Int("ZREPL_REPLICATION_MAX_ATTEMPTS", 3),
		OneStep:              in.OneStep,
		ReconnectHardFailTimeout: envconst.Duration(
			"ZREPL_REPLICATION_RECONNECT_HARD_FAIL_TIMEOUT", 10*time.Minute),
	}
	return c, c.Validate()
}

func activeSide(g *config.Global, in *config.ActiveJob, configJob any,
	connecter *Connecter,
) (*ActiveSide, error) {
	name, err := endpoint.MakeJobID(in.Name)
	if err != nil {
		return nil, fmt.Errorf("invalid job name: %w", err)
	}
	j := &ActiveSide{name: name}

	switch v := configJob.(type) {
	case *config.PushJob:
		j.mode, err = modePushFromConfig(g, v, j.name) // shadow
	case *config.PullJob:
		j.mode, err = modePullFromConfig(v, j.name) // shadow
	default:
		panic(fmt.Sprintf("implementation error: unknown job type %T", v))
	}
	if err != nil {
		return nil, err // no wrapping required
	}

	j.promRepStateSecs = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   "zrepl",
		Subsystem:   "replication",
		Name:        "state_time",
		Help:        "seconds spent during replication",
		ConstLabels: prometheus.Labels{"zrepl_job": j.name.String()},
	}, []string{"state"})

	j.promBytesReplicated = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   "zrepl",
		Subsystem:   "replication",
		Name:        "bytes_replicated",
		Help:        "number of bytes replicated from sender to receiver per filesystem",
		ConstLabels: prometheus.Labels{"zrepl_job": j.name.String()},
	}, []string{"filesystem"})

	j.promReplicationErrors = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace:   "zrepl",
		Subsystem:   "replication",
		Name:        "filesystem_errors",
		Help:        "number of filesystems that failed replication in the latest replication attempt, or -1 if the job failed before enumerating the filesystems",
		ConstLabels: prometheus.Labels{"zrepl_job": j.name.String()},
	})

	j.promLastSuccessful = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace:   "zrepl",
		Subsystem:   "replication",
		Name:        "last_successful",
		Help:        "timestamp of last successful replication",
		ConstLabels: prometheus.Labels{"zrepl_job": j.name.String()},
	})

	j.promPruneSecs = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   "zrepl",
		Subsystem:   "pruning",
		Name:        "time",
		Help:        "seconds spent in pruner",
		ConstLabels: prometheus.Labels{"zrepl_job": j.name.String()},
	}, []string{"prune_side"})

	j.prunerFactory, err = pruner.NewPrunerFactory(in.Pruning, j.promPruneSecs)
	if err != nil {
		return nil, err
	}

	j.replicationDriverConfig, err = replicationDriverConfigFromConfig(&in.Replication)
	if err != nil {
		return nil, fmt.Errorf("cannot build replication driver config: %w", err)
	}

	if in.Hooks.Pre != nil {
		j.preHook = NewHookFromConfig(in.Hooks.Pre)
	}
	if in.Hooks.Post != nil {
		j.postHook = NewHookFromConfig(in.Hooks.Post).WithPostHook(true)
	}

	if j.connected, err = connecter.FromConfig(&in.Connect); err != nil {
		return nil, fmt.Errorf("cannot build connect: %w", err)
	}
	return j, nil
}

func (j *ActiveSide) RegisterMetrics(registerer prometheus.Registerer) {
	registerer.MustRegister(j.promRepStateSecs)
	registerer.MustRegister(j.promPruneSecs)
	registerer.MustRegister(j.promBytesReplicated)
	registerer.MustRegister(j.promReplicationErrors)
	registerer.MustRegister(j.promLastSuccessful)
}

func (j *ActiveSide) Name() string { return j.name.String() }

func (j *ActiveSide) Runnable() bool { return true }

func (j *ActiveSide) Status() *Status {
	tasks := j.updateTasks(nil)
	s := &ActiveSideStatus{
		CronSpec:     j.mode.Cron(),
		StartedAt:    tasks.startedAt,
		Snapshotting: j.mode.SnapperReport(),
	}

	if id := j.cronId; id > 0 {
		s.SleepUntil = j.cron.Entry(id).Next
	}

	switch {
	case tasks.err != nil:
		s.Err = tasks.err.Error()
	case j.wakeupBusy > 0:
		s.Err = fmt.Sprintf(
			"job frequency is too high; replication was not done %d times",
			j.wakeupBusy)
	}

	if tasks.replicationReport != nil {
		s.Replication = tasks.replicationReport()
	}
	if tasks.prunerSender != nil {
		s.PruningSender = tasks.prunerSender.Report()
	}
	if tasks.prunerReceiver != nil {
		s.PruningReceiver = tasks.prunerReceiver.Report()
	}
	return &Status{Type: j.mode.Type(), JobSpecific: s}
}

type ActiveSideStatus struct {
	CronSpec   string
	SleepUntil time.Time
	StartedAt  time.Time
	Err        string

	Replication                    *report.Report
	PruningSender, PruningReceiver *pruner.Report
	Snapshotting                   *snapper.Report
}

func (self *ActiveSideStatus) Error() string {
	if self.Err != "" {
		return self.Err
	}

	if snap := self.Snapshotting; snap != nil {
		if s := snap.Error(); s != "" {
			return s
		}
	}

	if repl := self.Replication; repl != nil {
		if s := repl.Error(); s != "" {
			return s
		}
	}

	if prun := self.PruningSender; prun != nil {
		if prun.Error != "" {
			return prun.Error
		}
		for _, fs := range prun.Completed {
			if fs.SkipReason.NotSkipped() && fs.LastError != "" {
				return fs.LastError
			}
		}
	}

	if prun := self.PruningReceiver; prun != nil {
		if prun.Error != "" {
			return prun.Error
		}
		for _, fs := range prun.Completed {
			if fs.SkipReason.NotSkipped() && fs.LastError != "" {
				return fs.LastError
			}
		}
	}
	return ""
}

func (self *ActiveSideStatus) Running() (d time.Duration, ok bool) {
	if s := self.Snapshotting; s != nil {
		if d, ok = s.Running(); ok {
			return
		}
	}

	if r := self.Replication; r != nil {
		if d == 0 {
			d, ok = r.Running()
		} else {
			_, ok = r.Running()
		}
	}

	if p := self.PruningSender; p != nil {
		if d == 0 {
			d, ok = p.Running()
		} else {
			_, ok = p.Running()
		}
	}

	if p := self.PruningReceiver; p != nil {
		if d == 0 {
			d, ok = p.Running()
		} else {
			_, ok = p.Running()
		}
	}
	return
}

func (self *ActiveSideStatus) Cron() string {
	if self.CronSpec != "" {
		return self.CronSpec
	} else if self.Snapshotting != nil {
		return self.Snapshotting.Cron()
	}
	return ""
}

func (self *ActiveSideStatus) SleepingUntil() time.Time {
	if !self.SleepUntil.IsZero() {
		return self.SleepUntil
	} else if snap := self.Snapshotting; snap != nil {
		return snap.SleepingUntil()
	}
	return time.Time{}
}

func (self *ActiveSideStatus) Steps() (expected, step int) {
	expected = 3
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

	if r := self.Replication; r != nil {
		if d, ok := r.Running(); ok || d > 0 {
			step++
		}
	}

	if p := self.PruningSender; p != nil {
		if d, ok := p.Running(); ok || d > 0 {
			step++
		}
	} else if p := self.PruningReceiver; p != nil {
		if d, ok := p.Running(); ok || d > 0 {
			step++
		}
	}
	return
}

func (self *ActiveSideStatus) Progress() (expected, completed uint64) {
	if s := self.Snapshotting; s != nil {
		if _, ok := s.Running(); ok {
			return s.Progress()
		}
	}

	if r := self.Replication; r != nil {
		if _, ok := r.Running(); ok {
			return r.Progress()
		}
	}

	if p := self.PruningSender; p != nil {
		if _, ok := p.Running(); ok {
			expected, completed = p.Progress()
		}
	}

	if p := self.PruningReceiver; p != nil {
		if _, ok := p.Running(); ok {
			expected2, completed2 := p.Progress()
			expected += expected2
			completed += completed2
		}
	}
	return
}

func (j *ActiveSide) OwnedDatasetSubtreeRoot() (rfs *zfs.DatasetPath, ok bool) {
	pull, ok := j.mode.(*modePull)
	if !ok {
		_ = j.mode.(*modePush) // make sure we didn't introduce a new job type
		return nil, false
	}
	return pull.receiverConfig.RootWithoutClientComponent.Copy(), true
}

func (j *ActiveSide) SenderConfig() *endpoint.SenderConfig {
	push, ok := j.mode.(*modePush)
	if !ok {
		// make sure we didn't introduce a new job type
		_ = j.mode.(*modePull)
		return nil
	}
	return push.senderConfig
}

func (j *ActiveSide) Run(ctx context.Context, cron *cron.Cron) error {
	log := GetLogger(ctx)
	defer log.Info("job exiting")

	periodicDone := make(chan struct{})
	wakeupSig := j.runPeriodic(ctx, periodicDone, cron)

	j.running, j.shutdown = context.WithCancel(context.Background())
	defer j.shutdown()

forLoop:
	for {
		log.Info("wait for wakeups")
		select {
		case <-ctx.Done():
			log.WithError(ctx.Err()).Info("context")
			break forLoop
		case <-wakeupSig:
			j.mode.ResetConnectBackoff()
		case <-periodicDone:
		case <-j.running.Done():
			log.Info("shutdown received")
			break forLoop
		}
		j.do(ctx)
	}
	j.wait(log)
	return nil
}

func (j *ActiveSide) runPeriodic(ctx context.Context,
	wakeUpCommon chan<- struct{}, cron *cron.Cron,
) <-chan struct{} {
	if j.mode.PeriodicSnapshots() {
		return j.mode.RunSnapper(ctx, wakeUpCommon, cron)
	}

	cronSpec := j.mode.Cron()
	log := GetLogger(ctx).WithField("cron", cronSpec)
	id, err := cron.AddFunc(cronSpec, func() {
		select {
		case wakeUpCommon <- struct{}{}:
			j.wakeupBusy = 0
		case <-ctx.Done():
		default:
			j.wakeupBusy++
			log.Warn("job took longer than its interval")
		}
	})
	if err != nil {
		log.WithError(err).Error("failed add cron job")
	} else {
		j.cron, j.cronId = cron, id
		log.WithField("id", id).Info("add cron job")
	}
	return wakeup.Wait(ctx) // caller will handle wakeup signal
}

func (j *ActiveSide) do(ctx context.Context) {
	j.mode.ConnectEndpoints(ctx, j.connected)
	defer j.mode.DisconnectEndpoints()

	// allow cancellation of an invocation (this function)
	ctx, cancelThisRun := context.WithCancel(ctx)
	defer cancelThisRun()
	log := GetLogger(ctx)
	go func() {
		select {
		case <-reset.Wait(ctx):
			log.Info("reset received, cancelling current invocation")
			cancelThisRun()
		case <-ctx.Done():
		}
	}()

	running, shutdown := context.WithCancelCause(ctx)
	stopShutdown := context.AfterFunc(j.running, func() {
		log.Info("shutdown received, gracefully stop current invocation")
		shutdown(errors.New("shutdown recevived"))
	})
	defer func() {
		stopShutdown()
		shutdown(nil)
	}()

	steps := []func(context.Context) error{
		func(context.Context) error { return j.beforeReplication(ctx) },
		func(context.Context) error { return j.replicate(ctx) },
		j.pruneSender, j.pruneReceiver,
		func(context.Context) error { return j.afterPruning(ctx) },
	}
	if j.activeSteps(running, steps) {
		log.Info("task completed")
	}
}

func (j *ActiveSide) activeSteps(ctx context.Context,
	steps []func(ctx context.Context) error,
) bool {
	defer func() {
		j.updateTasks(func(tasks *activeSideTasks) {
			tasks.state = ActiveSideDone
		})
	}()

	for _, fn := range steps {
		if ctx.Err() != nil {
			return false
		} else if err := fn(ctx); err != nil {
			return false
		}
	}
	return true
}

func (j *ActiveSide) beforeReplication(ctx context.Context) error {
	j.updateTasks(func(tasks *activeSideTasks) {
		// reset it
		*tasks = activeSideTasks{
			state:     ActiveSideReplicating,
			startedAt: time.Now(),
		}
	})

	h := j.preHook
	if h == nil {
		return nil
	}
	log := GetLogger(ctx)
	log.Info("run pre hook")

	if err := h.Run(ctx, j); err != nil {
		log.WithField("err_is_fatal", h.ErrIsFatal()).
			WithError(err).Error("pre hook exited with error")
		err = fmt.Errorf("pre hook exited with error: %w", err)
		j.updateTasks(func(tasks *activeSideTasks) { tasks.err = err })
		if h.ErrIsFatal() {
			return err
		}
	}
	return nil
}

func (j *ActiveSide) replicate(ctx context.Context) error {
	log := GetLogger(ctx)
	log.Info("start replication")

	var repWait driver.WaitFunc
	sender, receiver := j.mode.SenderReceiver()
	j.updateTasks(func(tasks *activeSideTasks) {
		tasks.replicationReport, repWait = replication.Do(
			ctx, j.replicationDriverConfig, logic.NewPlanner(
				j.promRepStateSecs, j.promBytesReplicated, sender, receiver,
				j.mode.PlannerPolicy()),
			j.running)
	})
	repWait(true) // wait blocking

	replicationReport := j.tasks.replicationReport()
	numErrors := replicationReport.GetFailedFilesystemsCountInLatestAttempt()
	j.promReplicationErrors.Set(float64(numErrors))
	if numErrors == 0 {
		j.promLastSuccessful.SetToCurrentTime()
	}
	log.Info("finished replication")
	return nil
}

func (j *ActiveSide) pruneSender(ctx context.Context) error {
	log := GetLogger(ctx)
	log.WithField("concurrency", j.prunerFactory.Concurrency()).
		Info("start pruning sender")

	sender, _ := j.mode.SenderReceiver()
	tasks := j.updateTasks(func(tasks *activeSideTasks) {
		tasks.state = ActiveSidePruneSender
		tasks.prunerSender = j.prunerFactory.BuildSenderPruner(
			ctx, sender, sender)
	})

	begin := time.Now()
	tasks.prunerSender.Prune()
	log.WithField("duration", time.Since(begin)).
		Info("finished pruning sender")
	return nil
}

func (j *ActiveSide) pruneReceiver(ctx context.Context) error {
	log := GetLogger(ctx)
	log.WithField("concurrency", j.prunerFactory.Concurrency()).
		Info("start pruning receiver")

	sender, receiver := j.mode.SenderReceiver()
	tasks := j.updateTasks(func(tasks *activeSideTasks) {
		tasks.prunerReceiver = j.prunerFactory.BuildReceiverPruner(
			ctx, receiver, sender)
		tasks.state = ActiveSidePruneReceiver
	})

	begin := time.Now()
	tasks.prunerReceiver.Prune()
	log.WithField("duration", time.Since(begin)).
		Info("finished pruning receiver")
	return nil
}

func (j *ActiveSide) afterPruning(ctx context.Context) error {
	if j.postHook != nil {
		log := GetLogger(ctx)
		log.Info("run post hook")
		if err := j.postHook.Run(ctx, j); err != nil {
			log.WithError(err).Error("post hook exited with error")
			j.updateTasks(func(tasks *activeSideTasks) {
				tasks.err = fmt.Errorf("post hook exited with error: %w", err)
			})
		}
	}
	return nil
}

func (j *ActiveSide) Shutdown() {
	if j.mode.PeriodicSnapshots() {
		j.mode.Shutdown()
	}
	j.shutdown()
}

func (j *ActiveSide) wait(l logger.Logger) {
	if j.mode.PeriodicSnapshots() {
		l = l.WithField("mode", j.mode.Type())
		l.Info("waiting for snapper exit")
		defer l.Info("snapper exited")
	}
	j.mode.Wait()
}

func (j *ActiveSide) Running() (d time.Duration, ok bool) {
	tasks := j.updateTasks(nil)
	if !tasks.startedAt.IsZero() {
		d = time.Since(tasks.startedAt)
	}
	switch tasks.state {
	case ActiveSideReplicating, ActiveSidePruneSender, ActiveSidePruneReceiver:
		ok = true
	}
	return
}
