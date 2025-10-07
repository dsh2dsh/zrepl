package job

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/dsh2dsh/cron/v3"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/config/env"
	"github.com/dsh2dsh/zrepl/internal/daemon/job/signal"
	"github.com/dsh2dsh/zrepl/internal/daemon/pruner"
	"github.com/dsh2dsh/zrepl/internal/daemon/snapper"
	"github.com/dsh2dsh/zrepl/internal/endpoint"
	"github.com/dsh2dsh/zrepl/internal/logger"
	"github.com/dsh2dsh/zrepl/internal/replication/driver"
	"github.com/dsh2dsh/zrepl/internal/replication/logic"
	"github.com/dsh2dsh/zrepl/internal/replication/report"
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

	preHook  *Hook
	postHook *Hook
}

var _ Job = (*ActiveSide)(nil)

type ActiveSideState int

const (
	ActiveSideDone ActiveSideState = iota
	ActiveSideSnapshot
	ActiveSideReplicating
	ActiveSidePruneSender
	ActiveSidePruneReceiver
)

func (self ActiveSideState) String() string {
	switch self {
	case ActiveSideSnapshot:
		return "ActiveSideSnapshot"
	case ActiveSideReplicating:
		return "ActiveSideReplicating"
	case ActiveSidePruneSender:
		return "ActiveSidePruneSender"
	case ActiveSidePruneReceiver:
		return "ActiveSidePruneReceiver"
	case ActiveSideDone:
		return "ActiveSideDone"
	}
	return "ActiveSideState(" + strconv.FormatInt(int64(self), 10) + ")"
}

type activeSideTasks struct {
	state     ActiveSideState
	startedAt time.Time
	err       error

	// valid for state ActiveSideReplicating, ActiveSidePruneSender,
	// ActiveSidePruneReceiver, ActiveSideDone
	replicationReport driver.ReportFunc

	// valid for state ActiveSidePruneSender, ActiveSidePruneReceiver,
	// ActiveSideDone
	prunerSender, prunerReceiver *pruner.Pruner
}

func (a *ActiveSide) updateTasks(u func(*activeSideTasks)) activeSideTasks {
	a.tasksMtx.Lock()
	defer a.tasksMtx.Unlock()
	cloned := a.tasks
	if u == nil {
		return cloned
	}
	u(&cloned)
	a.tasks = cloned
	return cloned
}

type activeMode interface {
	ConnectEndpoints(ctx context.Context, cn Connected)
	DisconnectEndpoints()
	SenderReceiver() (logic.Sender, logic.Receiver)
	Type() Type
	PlannerPolicy() logic.PlannerPolicy
	Runnable() bool
	Periodic() bool
	Run(ctx context.Context)
	Cron() string
	Report() *snapper.Report
	Running() (time.Duration, bool)
}

func modePushFromConfig(g *config.Global, in *config.PushJob,
	jobID endpoint.JobID,
) (*modePush, error) {
	m := &modePush{
		drySendConcurrency: int(in.Replication.Concurrency.SizeEstimates),
		pruneConcurrency:   int(in.Pruning.Concurrency),
	}
	var err error
	m.senderConfig, err = buildSenderConfig(in, jobID)
	if err != nil {
		return nil, fmt.Errorf("sender config: %w", err)
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
		ConflictResolution: conflictResolution,
		ReplicationConfig:  replicationConfig,
	}
	if err := m.plannerPolicy.Validate(); err != nil {
		return nil, fmt.Errorf("cannot build planner policy: %w", err)
	}

	m.snapper, err = snapper.FromConfig(g, m.senderConfig.FSF, in.Snapshotting)
	if err != nil {
		return nil, fmt.Errorf("cannot build snapper: %w", err)
	}

	if cronSpec := m.snapper.Cron(); cronSpec != "" {
		if in.CronSpec() != "" {
			return nil, fmt.Errorf(
				"both cron spec and periodic snapshotting defined: %q", cronSpec)
		}
		m.cronSpec = cronSpec
	} else if cronSpec := in.CronSpec(); cronSpec != "" {
		if _, err := cron.ParseStandard(cronSpec); err != nil {
			return nil, fmt.Errorf("failed parse cron spec %q: %w", cronSpec, err)
		}
		m.cronSpec = cronSpec
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

	drySendConcurrency int
	pruneConcurrency   int
}

var _ activeMode = (*modePush)(nil)

func (m *modePush) ConnectEndpoints(ctx context.Context, cn Connected) {
	m.setupMtx.Lock()
	defer m.setupMtx.Unlock()

	if m.receiver != nil || m.sender != nil {
		panic("inconsistent use of ConnectEndpoints and DisconnectEndpoints")
	}

	GetLogger(ctx).With(
		slog.String("mode", string(m.Type())),
		slog.String("to", cn.Name()),
	).Info("connect to receiver")

	m.receiver = cn.Endpoint()
	m.sender = endpoint.NewSender(*m.senderConfig).
		WithDrySendConcurrency(m.drySendConcurrency).
		WithPruneConcurrency(m.pruneConcurrency)
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

func (m *modePush) Runnable() bool { return m.snapper.Runnable() }

func (m *modePush) Periodic() bool { return m.snapper.Periodic() }

func (m *modePush) Run(ctx context.Context) { m.snapper.Run(ctx) }

func (m *modePush) Report() *snapper.Report {
	r := m.snapper.Report()
	return &r
}

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

	pruneConcurrency int
}

var _ activeMode = (*modePull)(nil)

func (m *modePull) ConnectEndpoints(ctx context.Context, cn Connected) {
	m.setupMtx.Lock()
	defer m.setupMtx.Unlock()
	if m.receiver != nil || m.sender != nil {
		panic("inconsistent use of ConnectEndpoints and DisconnectEndpoints")
	}

	GetLogger(ctx).With(
		slog.String("mode", string(m.Type())),
		slog.String("from", cn.Name()),
	).Info("connect to sender")

	m.receiver = endpoint.NewReceiver(m.receiverConfig).
		WithPruneConcurrency(m.pruneConcurrency)
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

func (m *modePull) Runnable() bool { return false }

func (m *modePull) Periodic() bool { return false }

func (m *modePull) Run(ctx context.Context) {}

func (m *modePull) Report() *snapper.Report { return nil }

func (m *modePull) Running() (time.Duration, bool) { return 0, false }

func modePullFromConfig(in *config.PullJob, jobID endpoint.JobID,
) (m *modePull, err error) {
	if in.Connect.Type == "local" || in.Connect.Server == "" {
		return nil, fmt.Errorf("pull job %q cannot use local connect", jobID)
	}

	m = &modePull{pruneConcurrency: int(in.Pruning.Concurrency)}
	if cronSpec := in.CronSpec(); cronSpec != "" {
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
		ConflictResolution: conflictResolution,
		ReplicationConfig:  replicationConfig,
	}
	if err := m.plannerPolicy.Validate(); err != nil {
		return nil, fmt.Errorf("cannot build planner policy: %w", err)
	}

	m.receiverConfig, err = buildReceiverConfig(in, jobID)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func replicationDriverConfigFromConfig(in *config.Replication) (driver.Config,
	error,
) {
	c := driver.Config{
		StepQueueConcurrency:     in.Concurrency.Steps,
		MaxAttempts:              env.Values.ReplicationMaxAttempts,
		Prefix:                   in.Prefix,
		ReconnectHardFailTimeout: env.Values.ReplicationReconnectHardTimeout,
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

func (j *ActiveSide) Cron() string { return j.mode.Cron() }

func (j *ActiveSide) Runnable() bool { return j.mode.Runnable() }

func (j *ActiveSide) Status() *Status {
	tasks := j.updateTasks(nil)
	s := &ActiveSideStatus{
		CronSpec:     j.mode.Cron(),
		StartedAt:    tasks.startedAt,
		Snapshotting: j.mode.Report(),
	}

	if tasks.err != nil {
		s.Err = tasks.err.Error()
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
	CronSpec  string
	StartedAt time.Time
	Err       string

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
			return d, ok
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
	return d, ok
}

func (self *ActiveSideStatus) Cron() string {
	if self.CronSpec != "" {
		return self.CronSpec
	} else if self.Snapshotting != nil {
		return self.Snapshotting.Cron()
	}
	return ""
}

func (self *ActiveSideStatus) SleepingUntil() (sleepUntil time.Time) {
	if snap := self.Snapshotting; snap != nil {
		sleepUntil = snap.SleepingUntil()
	}
	return sleepUntil
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
	return expected, step
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
	return expected, completed
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

func (j *ActiveSide) Run(ctx context.Context) error {
	log := GetLogger(ctx)
	defer log.Info("job exiting")

	j.mode.ConnectEndpoints(ctx, j.connected)
	defer j.mode.DisconnectEndpoints()

	steps := []func(context.Context) error{
		func(context.Context) error { return j.before(ctx) },
		j.snapshot,
		func(context.Context) error { return j.replicate(ctx) },
		j.pruneSender,
		j.pruneReceiver,
		func(context.Context) error { return j.afterPruning(ctx) },
	}

	if j.activeSteps(signal.GracefulFrom(ctx), steps) {
		log.Info("task completed")
	}
	return nil
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

func (j *ActiveSide) before(ctx context.Context) error {
	j.updateTasks(func(tasks *activeSideTasks) {
		// reset it
		*tasks = activeSideTasks{
			state:     ActiveSideSnapshot,
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
		logger.WithError(
			log.With(slog.Bool("err_is_fatal", h.ErrIsFatal())), err,
			"pre hook exited with error")
		err = fmt.Errorf("pre hook exited with error: %w", err)
		j.updateTasks(func(tasks *activeSideTasks) { tasks.err = err })
		if h.ErrIsFatal() {
			return err
		}
	}
	return nil
}

func (j *ActiveSide) snapshot(ctx context.Context) error {
	if !j.mode.Periodic() {
		return nil
	}

	log := GetLogger(ctx)
	log.Info("start snapshotting")
	j.mode.Run(ctx)
	log.Info("finished snapshotting")
	return nil
}

func (j *ActiveSide) replicate(ctx context.Context) error {
	log := GetLogger(ctx)
	log.Info("start replication")

	var repWait driver.WaitFunc
	sender, receiver := j.mode.SenderReceiver()
	j.updateTasks(func(tasks *activeSideTasks) {
		tasks.state = ActiveSideReplicating
		tasks.replicationReport, repWait = driver.Do(
			ctx, j.replicationDriverConfig, logic.NewPlanner(
				j.promRepStateSecs, j.promBytesReplicated, sender, receiver,
				j.mode.PlannerPolicy()))
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
	sender, _ := j.mode.SenderReceiver()
	senderOnce := NewSenderOnce(ctx, sender)
	tasks := j.updateTasks(func(tasks *activeSideTasks) {
		tasks.state = ActiveSidePruneSender
		tasks.prunerSender = j.prunerFactory.BuildSenderPruner(
			ctx, senderOnce, senderOnce)
	})

	log := GetLogger(ctx)
	log.With(slog.Int("concurrency", tasks.prunerSender.Concurrency())).
		Info("start pruning sender")

	begin := time.Now()
	tasks.prunerSender.Prune()
	log.With(slog.Duration("duration", time.Since(begin))).
		Info("finished pruning sender")
	return nil
}

func (j *ActiveSide) pruneReceiver(ctx context.Context) error {
	sender, receiver := j.mode.SenderReceiver()
	tasks := j.updateTasks(func(tasks *activeSideTasks) {
		tasks.prunerReceiver = j.prunerFactory.BuildReceiverPruner(
			ctx, receiver, sender)
		tasks.state = ActiveSidePruneReceiver
	})

	log := GetLogger(ctx)
	log.With(slog.Int("concurrency", tasks.prunerReceiver.Concurrency())).
		Info("start pruning receiver")

	begin := time.Now()
	tasks.prunerReceiver.Prune()
	log.With(slog.Duration("duration", time.Since(begin))).
		Info("finished pruning receiver")
	return nil
}

func (j *ActiveSide) afterPruning(ctx context.Context) error {
	if j.postHook != nil {
		log := GetLogger(ctx)
		log.Info("run post hook")
		if err := j.postHook.Run(ctx, j); err != nil {
			logger.WithError(log, err, "post hook exited with error")
			j.updateTasks(func(tasks *activeSideTasks) {
				tasks.err = fmt.Errorf("post hook exited with error: %w", err)
			})
		}
	}
	return nil
}

func (j *ActiveSide) Running() (d time.Duration, ok bool) {
	tasks := j.updateTasks(nil)
	if !tasks.startedAt.IsZero() {
		d = time.Since(tasks.startedAt)
	}
	ok = tasks.state != ActiveSideDone
	return d, ok
}
