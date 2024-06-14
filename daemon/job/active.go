package job

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dsh2dsh/cron/v3"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/dsh2dsh/zrepl/config"
	"github.com/dsh2dsh/zrepl/daemon/job/reset"
	"github.com/dsh2dsh/zrepl/daemon/job/wakeup"
	"github.com/dsh2dsh/zrepl/daemon/logging/trace"
	"github.com/dsh2dsh/zrepl/daemon/pruner"
	"github.com/dsh2dsh/zrepl/daemon/snapper"
	"github.com/dsh2dsh/zrepl/endpoint"
	"github.com/dsh2dsh/zrepl/replication"
	"github.com/dsh2dsh/zrepl/replication/driver"
	"github.com/dsh2dsh/zrepl/replication/logic"
	"github.com/dsh2dsh/zrepl/replication/report"
	"github.com/dsh2dsh/zrepl/rpc"
	"github.com/dsh2dsh/zrepl/transport"
	"github.com/dsh2dsh/zrepl/transport/fromconfig"
	"github.com/dsh2dsh/zrepl/util/envconst"
	"github.com/dsh2dsh/zrepl/zfs"
)

type ActiveSide struct {
	mode      activeMode
	name      endpoint.JobID
	connecter transport.Connecter

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
	state ActiveSideState

	// valid for state ActiveSideReplicating, ActiveSidePruneSender, ActiveSidePruneReceiver, ActiveSideDone
	replicationReport driver.ReportFunc
	replicationCancel context.CancelFunc

	// valid for state ActiveSidePruneSender, ActiveSidePruneReceiver, ActiveSideDone
	prunerSender, prunerReceiver *pruner.Pruner

	// valid for state ActiveSidePruneReceiver, ActiveSideDone
	prunerSenderCancel, prunerReceiverCancel context.CancelFunc
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
	ConnectEndpoints(ctx context.Context, connecter transport.Connecter)
	DisconnectEndpoints()
	SenderReceiver() (logic.Sender, logic.Receiver)
	Type() Type
	PlannerPolicy() logic.PlannerPolicy
	RunPeriodic(ctx context.Context, wakeUpCommon chan<- struct{},
		cron *cron.Cron) <-chan struct{}
	Cron() string
	SnapperReport() *snapper.Report
	ResetConnectBackoff()
}

type modePush struct {
	setupMtx      sync.Mutex
	sender        *endpoint.Sender
	receiver      *rpc.Client
	senderConfig  *endpoint.SenderConfig
	plannerPolicy *logic.PlannerPolicy
	snapper       snapper.Snapper
	cronSpec      string
}

func (m *modePush) ConnectEndpoints(ctx context.Context, connecter transport.Connecter) {
	m.setupMtx.Lock()
	defer m.setupMtx.Unlock()
	if m.receiver != nil || m.sender != nil {
		panic("inconsistent use of ConnectEndpoints and DisconnectEndpoints")
	}
	m.sender = endpoint.NewSender(*m.senderConfig)
	m.receiver = rpc.NewClient(connecter, rpc.GetLoggersOrPanic(ctx))
}

func (m *modePush) DisconnectEndpoints() {
	m.setupMtx.Lock()
	defer m.setupMtx.Unlock()
	m.receiver.Close()
	m.sender = nil
	m.receiver = nil
}

func (m *modePush) SenderReceiver() (logic.Sender, logic.Receiver) {
	m.setupMtx.Lock()
	defer m.setupMtx.Unlock()
	return m.sender, m.receiver
}

func (m *modePush) Type() Type { return TypePush }

func (m *modePush) PlannerPolicy() logic.PlannerPolicy { return *m.plannerPolicy }

func (m *modePush) Cron() string { return m.cronSpec }

func (m *modePush) RunPeriodic(ctx context.Context,
	wakeUpCommon chan<- struct{}, cron *cron.Cron,
) <-chan struct{} {
	if m.snapper.RunPeriodic() {
		go m.snapper.Run(ctx, wakeUpCommon, cron)
		return make(chan struct{}) // snapper will handle wakeup signal
	}
	GetLogger(ctx).Info("periodic snapshotting disabled")
	return wakeup.Wait(ctx) // caller will handle wakeup signal
}

func (m *modePush) SnapperReport() *snapper.Report {
	r := m.snapper.Report()
	return &r
}

func (m *modePush) ResetConnectBackoff() {
	m.setupMtx.Lock()
	defer m.setupMtx.Unlock()
	if m.receiver != nil {
		m.receiver.ResetConnectBackoff()
	}
}

func modePushFromConfig(g *config.Global, in *config.PushJob, jobID endpoint.JobID) (*modePush, error) {
	m := &modePush{}
	var err error

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

	m.senderConfig, err = buildSenderConfig(in, jobID)
	if err != nil {
		return nil, fmt.Errorf("sender config: %w", err)
	}

	replicationConfig, err := logic.ReplicationConfigFromConfig(&in.Replication)
	if err != nil {
		return nil, fmt.Errorf("field `replication`: %w", err)
	}

	conflictResolution, err := logic.ConflictResolutionFromConfig(&in.ConflictResolution)
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

	if m.snapper, err = snapper.FromConfig(g, m.senderConfig.FSF, in.Snapshotting); err != nil {
		return nil, fmt.Errorf("cannot build snapper: %w", err)
	}

	return m, nil
}

type modePull struct {
	setupMtx       sync.Mutex
	receiver       *endpoint.Receiver
	receiverConfig endpoint.ReceiverConfig
	sender         *rpc.Client
	plannerPolicy  *logic.PlannerPolicy
	cronSpec       string
}

func (m *modePull) ConnectEndpoints(ctx context.Context, connecter transport.Connecter) {
	m.setupMtx.Lock()
	defer m.setupMtx.Unlock()
	if m.receiver != nil || m.sender != nil {
		panic("inconsistent use of ConnectEndpoints and DisconnectEndpoints")
	}
	m.receiver = endpoint.NewReceiver(m.receiverConfig)
	m.sender = rpc.NewClient(connecter, rpc.GetLoggersOrPanic(ctx))
}

func (m *modePull) DisconnectEndpoints() {
	m.setupMtx.Lock()
	defer m.setupMtx.Unlock()
	m.sender.Close()
	m.sender = nil
	m.receiver = nil
}

func (m *modePull) SenderReceiver() (logic.Sender, logic.Receiver) {
	m.setupMtx.Lock()
	defer m.setupMtx.Unlock()
	return m.sender, m.receiver
}

func (*modePull) Type() Type { return TypePull }

func (m *modePull) PlannerPolicy() logic.PlannerPolicy { return *m.plannerPolicy }

func (m *modePull) Cron() string { return m.cronSpec }

func (m *modePull) RunPeriodic(ctx context.Context,
	wakeUpCommon chan<- struct{}, cron *cron.Cron,
) <-chan struct{} {
	GetLogger(ctx).Info("manual pull configured, periodic pull disabled")
	return wakeup.Wait(ctx) // caller will handle wakeup signal
}

func (m *modePull) SnapperReport() *snapper.Report {
	return nil
}

func (m *modePull) ResetConnectBackoff() {
	m.setupMtx.Lock()
	defer m.setupMtx.Unlock()
	if m.sender != nil {
		m.sender.ResetConnectBackoff()
	}
}

func modePullFromConfig(g *config.Global, in *config.PullJob, jobID endpoint.JobID) (m *modePull, err error) {
	m = &modePull{}

	cronSpec := in.CronSpec()
	if cronSpec != "" {
		if _, err := cron.ParseStandard(cronSpec); err != nil {
			return nil, fmt.Errorf("parse cron spec %q: %w", cronSpec, err)
		}
		m.cronSpec = cronSpec
	}

	replicationConfig, err := logic.ReplicationConfigFromConfig(&in.Replication)
	if err != nil {
		return nil, fmt.Errorf("field `replication`: %w", err)
	}

	conflictResolution, err := logic.ConflictResolutionFromConfig(&in.ConflictResolution)
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

func activeSide(g *config.Global, in *config.ActiveJob, configJob interface{}, parseFlags config.ParseFlags) (j *ActiveSide, err error) {
	j = &ActiveSide{}
	j.name, err = endpoint.MakeJobID(in.Name)
	if err != nil {
		return nil, fmt.Errorf("invalid job name: %w", err)
	}

	switch v := configJob.(type) {
	case *config.PushJob:
		j.mode, err = modePushFromConfig(g, v, j.name) // shadow
	case *config.PullJob:
		j.mode, err = modePullFromConfig(g, v, j.name) // shadow
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

	j.connecter, err = fromconfig.ConnecterFromConfig(g, in.Connect, parseFlags)
	if err != nil {
		return nil, fmt.Errorf("cannot build client: %w", err)
	}

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

func (j *ActiveSide) Status() *Status {
	tasks := j.updateTasks(nil)
	s := &ActiveSideStatus{
		CronSpec:     j.mode.Cron(),
		Snapshotting: j.mode.SnapperReport(),
	}

	if id := j.cronId; id > 0 {
		s.SleepUntil = j.cron.Entry(id).Next
	}

	if cnt := j.wakeupBusy; cnt > 0 {
		s.err = fmt.Errorf(
			"job frequency is too high; replication was not done %d times", cnt)
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

	s.Snapshotting = j.mode.SnapperReport()
	return &Status{Type: j.mode.Type(), JobSpecific: s}
}

type ActiveSideStatus struct {
	CronSpec   string
	SleepUntil time.Time
	err        error

	Replication                    *report.Report
	PruningSender, PruningReceiver *pruner.Report
	Snapshotting                   *snapper.Report
}

func (self *ActiveSideStatus) Error() string {
	if self.err != nil {
		return self.err.Error()
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
	return ""
}

func (self *ActiveSideStatus) Running() time.Duration {
	var d time.Duration
	if repl := self.Replication; repl != nil {
		d = repl.Running()
	}
	if snap := self.Snapshotting; snap != nil {
		if d2 := snap.Running(); d2 > d {
			d = d2
		}
	}
	return d
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
		_ = j.mode.(*modePull) // make sure we didn't introduce a new job type
		return nil
	}
	return push.senderConfig
}

// The active side of a replication uses one end (sender or receiver)
// directly by method invocation, without going through a transport that
// provides a client identity.
// However, in order to avoid the need to distinguish between direct-method-invocating
// clients and RPC client, we use an invalid client identity as a sentinel value.
func FakeActiveSideDirectMethodInvocationClientIdentity(jobId endpoint.JobID) string {
	return fmt.Sprintf("<local><active><job><client><identity><job=%q>", jobId.String())
}

func (j *ActiveSide) Run(ctx context.Context, cron *cron.Cron) {
	ctx, endTask := trace.WithTaskAndSpan(ctx, "active-side-job", j.Name())
	defer endTask()

	ctx = context.WithValue(ctx, endpoint.ClientIdentityKey,
		FakeActiveSideDirectMethodInvocationClientIdentity(j.name))

	log := GetLogger(ctx)

	defer log.Info("job exiting")

	periodicDone := make(chan struct{})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	periodicCtx, endTask := trace.WithTask(ctx, "periodic")
	defer endTask()
	wakeupSig := j.runPeriodic(periodicCtx, periodicDone, cron)

	invocationCount := 0
	for {
		log.Info("wait for wakeups")
		select {
		case <-ctx.Done():
		case <-wakeupSig:
			j.mode.ResetConnectBackoff()
		case <-periodicDone:
		}
		if ctx.Err() != nil {
			log.WithError(ctx.Err()).Info("context")
			return
		}
		invocationCount++
		invocationCtx, endSpan := trace.WithSpan(ctx,
			fmt.Sprintf("invocation-%d", invocationCount))
		j.do(invocationCtx)
		endSpan()
	}
}

func (j *ActiveSide) runPeriodic(ctx context.Context,
	wakeUpCommon chan<- struct{}, cron *cron.Cron,
) <-chan struct{} {
	cronSpec := j.mode.Cron()
	if cronSpec == "" {
		return j.mode.RunPeriodic(ctx, wakeUpCommon, cron)
	}

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
	j.mode.ConnectEndpoints(ctx, j.connecter)
	defer j.mode.DisconnectEndpoints()

	// allow cancellation of an invocation (this function)
	ctx, cancelThisRun := context.WithCancel(ctx)
	defer cancelThisRun()
	go func() {
		select {
		case <-reset.Wait(ctx):
			log := GetLogger(ctx)
			log.Info("reset received, cancelling current invocation")
			cancelThisRun()
		case <-ctx.Done():
		}
	}()

	sender, receiver := j.mode.SenderReceiver()

	{
		select {
		case <-ctx.Done():
			return
		default:
		}
		ctx, endSpan := trace.WithSpan(ctx, "replication")
		ctx, repCancel := context.WithCancel(ctx)
		var repWait driver.WaitFunc
		j.updateTasks(func(tasks *activeSideTasks) {
			// reset it
			*tasks = activeSideTasks{}
			tasks.replicationCancel = func() { repCancel(); endSpan() }
			tasks.replicationReport, repWait = replication.Do(
				ctx, j.replicationDriverConfig, logic.NewPlanner(j.promRepStateSecs, j.promBytesReplicated, sender, receiver, j.mode.PlannerPolicy()),
			)
			tasks.state = ActiveSideReplicating
		})
		GetLogger(ctx).Info("start replication")
		repWait(true) // wait blocking
		repCancel()   // always cancel to free up context resources

		replicationReport := j.tasks.replicationReport()
		numErrors := replicationReport.GetFailedFilesystemsCountInLatestAttempt()
		j.promReplicationErrors.Set(float64(numErrors))
		if numErrors == 0 {
			j.promLastSuccessful.SetToCurrentTime()
		}

		endSpan()
	}

	{
		select {
		case <-ctx.Done():
			return
		default:
		}
		ctx, endSpan := trace.WithSpan(ctx, "prune_sender")
		ctx, senderCancel := context.WithCancel(ctx)
		tasks := j.updateTasks(func(tasks *activeSideTasks) {
			tasks.prunerSender = j.prunerFactory.BuildSenderPruner(ctx, sender, sender)
			tasks.prunerSenderCancel = func() { senderCancel(); endSpan() }
			tasks.state = ActiveSidePruneSender
		})
		GetLogger(ctx).Info("start pruning sender")
		tasks.prunerSender.Prune()
		GetLogger(ctx).Info("finished pruning sender")
		senderCancel()
		endSpan()
	}
	{
		select {
		case <-ctx.Done():
			return
		default:
		}
		ctx, endSpan := trace.WithSpan(ctx, "prune_recever")
		ctx, receiverCancel := context.WithCancel(ctx)
		tasks := j.updateTasks(func(tasks *activeSideTasks) {
			tasks.prunerReceiver = j.prunerFactory.BuildReceiverPruner(ctx, receiver, sender)
			tasks.prunerReceiverCancel = func() { receiverCancel(); endSpan() }
			tasks.state = ActiveSidePruneReceiver
		})
		GetLogger(ctx).Info("start pruning receiver")
		tasks.prunerReceiver.Prune()
		GetLogger(ctx).Info("finished pruning receiver")
		receiverCancel()
		endSpan()
	}

	j.updateTasks(func(tasks *activeSideTasks) {
		tasks.state = ActiveSideDone
	})
}
