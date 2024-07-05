package job

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dsh2dsh/cron/v3"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/dsh2dsh/zrepl/config"
	"github.com/dsh2dsh/zrepl/daemon/logging"
	"github.com/dsh2dsh/zrepl/daemon/logging/trace"
	"github.com/dsh2dsh/zrepl/daemon/snapper"
	"github.com/dsh2dsh/zrepl/endpoint"
	"github.com/dsh2dsh/zrepl/logger"
	"github.com/dsh2dsh/zrepl/rpc"
	"github.com/dsh2dsh/zrepl/transport"
	"github.com/dsh2dsh/zrepl/transport/fromconfig"
	"github.com/dsh2dsh/zrepl/zfs"
)

type PassiveSide struct {
	mode   passiveMode
	name   endpoint.JobID
	listen transport.AuthenticatedListenerFactory

	wg       sync.WaitGroup
	shutdown context.CancelFunc
}

type passiveMode interface {
	Handler() rpc.Handler
	Periodic() bool
	RunPeriodic(ctx context.Context, cron *cron.Cron)
	SnapperReport() *snapper.Report // may be nil
	Type() Type
	Shutdown()
}

type modeSink struct {
	receiverConfig endpoint.ReceiverConfig
}

func (m *modeSink) Type() Type { return TypeSink }

func (m *modeSink) Handler() rpc.Handler {
	return endpoint.NewReceiver(m.receiverConfig)
}

func (m *modeSink) Periodic() bool { return false }

func (m *modeSink) RunPeriodic(_ context.Context, cron *cron.Cron) {}

func (m *modeSink) SnapperReport() *snapper.Report { return nil }

func (m *modeSink) Shutdown() {}

func modeSinkFromConfig(_ *config.Global, in *config.SinkJob,
	jobID endpoint.JobID,
) (m *modeSink, err error) {
	m = &modeSink{}

	m.receiverConfig, err = buildReceiverConfig(in, jobID)
	if err != nil {
		return nil, err
	}

	return m, nil
}

type modeSource struct {
	senderConfig *endpoint.SenderConfig
	snapper      snapper.Snapper
}

func modeSourceFromConfig(g *config.Global, in *config.SourceJob, jobID endpoint.JobID) (m *modeSource, err error) {
	// FIXME exact dedup of modePush
	m = &modeSource{}

	m.senderConfig, err = buildSenderConfig(in, jobID)
	if err != nil {
		return nil, fmt.Errorf("send options: %w", err)
	}

	if m.snapper, err = snapper.FromConfig(g, m.senderConfig.FSF, in.Snapshotting); err != nil {
		return nil, fmt.Errorf("cannot build snapper: %w", err)
	}

	return m, nil
}

func (m *modeSource) Type() Type { return TypeSource }

func (m *modeSource) Handler() rpc.Handler {
	return endpoint.NewSender(*m.senderConfig)
}

func (m *modeSource) Periodic() bool { return true }

func (m *modeSource) RunPeriodic(ctx context.Context, cron *cron.Cron) {
	m.snapper.Run(ctx, nil, cron)
}

func (m *modeSource) SnapperReport() *snapper.Report {
	r := m.snapper.Report()
	return &r
}

func (m *modeSource) Shutdown() {
	m.snapper.Shutdown()
}

func passiveSideFromConfig(g *config.Global, in *config.PassiveJob, configJob interface{}, parseFlags config.ParseFlags) (s *PassiveSide, err error) {
	s = &PassiveSide{}

	s.name, err = endpoint.MakeJobID(in.Name)
	if err != nil {
		return nil, fmt.Errorf("invalid job name: %w", err)
	}

	switch v := configJob.(type) {
	case *config.SinkJob:
		s.mode, err = modeSinkFromConfig(g, v, s.name) // shadow
	case *config.SourceJob:
		s.mode, err = modeSourceFromConfig(g, v, s.name) // shadow
	}
	if err != nil {
		return nil, err // no wrapping necessary
	}

	if s.listen, err = fromconfig.ListenerFactoryFromConfig(g, in.Serve, parseFlags); err != nil {
		return nil, fmt.Errorf("cannot build listener factory: %w", err)
	}

	return s, nil
}

func (j *PassiveSide) Name() string { return j.name.String() }

func (s *PassiveSide) Status() *Status {
	st := &PassiveStatus{
		Snapper: s.mode.SnapperReport(),
	}
	return &Status{Type: s.mode.Type(), JobSpecific: st}
}

type PassiveStatus struct {
	Snapper *snapper.Report
}

func (self *PassiveStatus) Error() string {
	if snap := self.Snapper; snap != nil {
		if s := snap.Error(); s != "" {
			return s
		}
	}
	return ""
}

func (self *PassiveStatus) Running() time.Duration {
	if snap := self.Snapper; snap != nil {
		return snap.Running()
	}
	return 0
}

func (j *PassiveSide) OwnedDatasetSubtreeRoot() (rfs *zfs.DatasetPath, ok bool) {
	sink, ok := j.mode.(*modeSink)
	if !ok {
		_ = j.mode.(*modeSource) // make sure we didn't introduce a new job type
		return nil, false
	}
	return sink.receiverConfig.RootWithoutClientComponent.Copy(), true
}

func (j *PassiveSide) SenderConfig() *endpoint.SenderConfig {
	source, ok := j.mode.(*modeSource)
	if !ok {
		_ = j.mode.(*modeSink) // make sure we didn't introduce a new job type
		return nil
	}
	return source.senderConfig
}

func (*PassiveSide) RegisterMetrics(registerer prometheus.Registerer) {}

func (j *PassiveSide) Run(ctx context.Context, cron *cron.Cron) {
	ctx, endTask := trace.WithTaskAndSpan(ctx, "passive-side-job", j.Name())
	defer endTask()

	log := GetLogger(ctx)
	defer log.Info("job exiting")

	j.goModePeriodic(ctx, cron)

	handler := j.mode.Handler()
	if handler == nil {
		panic(fmt.Sprintf(
			"implementation error: j.mode.Handler() returned nil: %#v", j))
	}

	rpcLoggers := rpc.GetLoggersOrPanic(ctx) // WithSubsystemLoggers above
	server := rpc.NewServer(handler, rpcLoggers, j.ctxInterceptor(ctx))

	listener, err := j.listen()
	if err != nil {
		log.WithError(err).Error("cannot listen")
		return
	}

	ctx, j.shutdown = context.WithCancel(ctx)
	defer j.shutdown()
	server.Serve(ctx, listener)
	j.wait(log)
}

func (j *PassiveSide) goModePeriodic(ctx context.Context, cron *cron.Cron) {
	if !j.mode.Periodic() {
		return
	}

	j.wg.Add(1)
	go func() {
		defer j.wg.Done()
		ctx, endTask := trace.WithTask(ctx, "periodic")
		j.mode.RunPeriodic(ctx, cron)
		endTask()
	}()
}

func (j *PassiveSide) ctxInterceptor(ctx context.Context,
) rpc.HandlerContextInterceptor {
	return func(handlerCtx context.Context,
		info rpc.HandlerContextInterceptorData, handler func(ctx context.Context),
	) {
		// the handlerCtx is clean => need to inherit logging and tracing config
		// from job context
		handlerCtx = logging.WithInherit(handlerCtx, ctx)
		handlerCtx = trace.WithInherit(handlerCtx, ctx)
		handlerCtx, endTask := trace.WithTaskAndSpan(handlerCtx, "handler",
			fmt.Sprintf("job=%q client=%q method=%q", j.Name(),
				info.ClientIdentity(), info.FullMethod()))
		handler(handlerCtx)
		endTask()
	}
}

func (j *PassiveSide) wait(l logger.Logger) {
	l = l.WithField("mode", j.mode.Type())
	l.Info("waiting for mode job exit")
	defer l.Info("mode job exited")
	if j.mode.Periodic() {
	}
	j.wg.Wait()
}

func (j *PassiveSide) Shutdown() {
	j.mode.Shutdown()
	j.shutdown()
}
