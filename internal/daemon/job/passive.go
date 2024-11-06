package job

import (
	"context"
	"fmt"
	"time"

	"github.com/dsh2dsh/cron/v3"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/daemon/snapper"
	"github.com/dsh2dsh/zrepl/internal/endpoint"
	"github.com/dsh2dsh/zrepl/internal/zfs"
)

type PassiveSide struct {
	mode passiveMode
	name endpoint.JobID
}

type passiveMode interface {
	Endpoint(clientIdentity string) Endpoint
	Periodic() bool
	RunPeriodic(ctx context.Context, cron *cron.Cron)
	SnapperReport() *snapper.Report // may be nil
	Type() Type
	Shutdown()
}

func modeSinkFromConfig(in *config.SinkJob, jobID endpoint.JobID,
) (*modeSink, error) {
	c, err := buildReceiverConfig(in, jobID)
	if err != nil {
		return nil, err
	}
	m := &modeSink{receiverConfig: c}
	return m, nil
}

type modeSink struct {
	receiverConfig endpoint.ReceiverConfig
}

var _ passiveMode = (*modeSink)(nil)

func (m *modeSink) Type() Type { return TypeSink }

func (m *modeSink) Periodic() bool { return false }

func (m *modeSink) RunPeriodic(_ context.Context, cron *cron.Cron) {}

func (m *modeSink) SnapperReport() *snapper.Report { return nil }

func (m *modeSink) Shutdown() {}

func (m *modeSink) Endpoint(clientIdentity string) Endpoint {
	return endpoint.NewReceiver(m.receiverConfig).
		WithClientIdentity(clientIdentity)
}

func modeSourceFromConfig(g *config.Global, in *config.SourceJob,
	jobID endpoint.JobID,
) (m *modeSource, err error) {
	// FIXME exact dedup of modePush
	m = &modeSource{}
	if m.senderConfig, err = buildSenderConfig(in, jobID); err != nil {
		return nil, fmt.Errorf("send options: %w", err)
	}

	m.snapper, err = snapper.FromConfig(g, m.senderConfig.FSF, in.Snapshotting)
	if err != nil {
		return nil, fmt.Errorf("cannot build snapper: %w", err)
	}
	return m, nil
}

type modeSource struct {
	senderConfig *endpoint.SenderConfig
	snapper      snapper.Snapper
}

var _ passiveMode = (*modeSource)(nil)

func (m *modeSource) Type() Type { return TypeSource }

func (m *modeSource) Endpoint(clientIdentity string) Endpoint {
	return endpoint.NewSender(*m.senderConfig)
}

func (m *modeSource) Periodic() bool { return m.snapper.Periodic() }

func (m *modeSource) RunPeriodic(ctx context.Context, cron *cron.Cron) {
	m.snapper.Run(ctx, nil, cron)
}

func (m *modeSource) SnapperReport() *snapper.Report {
	r := m.snapper.Report()
	return &r
}

func (m *modeSource) Shutdown() { m.snapper.Shutdown() }

func passiveSideFromConfig(g *config.Global, in *config.PassiveJob,
	configJob any, connecter *Connecter,
) (*PassiveSide, error) {
	jobID, err := endpoint.MakeJobID(in.Name)
	if err != nil {
		return nil, fmt.Errorf("invalid job name: %w", err)
	}
	s := &PassiveSide{name: jobID}

	switch v := configJob.(type) {
	case *config.SinkJob:
		s.mode, err = modeSinkFromConfig(v, s.name) // shadow
	case *config.SourceJob:
		s.mode, err = modeSourceFromConfig(g, v, s.name) // shadow
	}
	if err != nil {
		return nil, err // no wrapping necessary
	}

	connecter.AddJob(s.Name(), s)
	return s, nil
}

func (j *PassiveSide) Name() string { return j.name.String() }

func (j *PassiveSide) Runnable() bool { return j.mode.Periodic() }

func (s *PassiveSide) Status() *Status {
	snapperReport := s.mode.SnapperReport()
	if snapperReport == nil || snapperReport.Type == snapper.TypeManual {
		return nil
	}
	return &Status{
		Type:        s.mode.Type(),
		JobSpecific: &PassiveStatus{Snapper: snapperReport},
	}
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

func (self *PassiveStatus) Running() (time.Duration, bool) {
	if snap := self.Snapper; snap != nil {
		return snap.Running()
	}
	return 0, false
}

func (self *PassiveStatus) Cron() string {
	if snap := self.Snapper; snap != nil {
		return snap.Cron()
	}
	return ""
}

func (self *PassiveStatus) SleepingUntil() time.Time {
	if snap := self.Snapper; snap != nil {
		return snap.SleepingUntil()
	}
	return time.Time{}
}

func (self *PassiveStatus) Steps() (expected, step int) {
	if s := self.Snapper; s != nil {
		expected++
		if _, ok := s.Running(); ok {
			step++
		}
	}
	return
}

func (self *PassiveStatus) Progress() (uint64, uint64) {
	if s := self.Snapper; s != nil {
		if _, ok := s.Running(); ok {
			return s.Progress()
		}
	}
	return 0, 0
}

func (j *PassiveSide) OwnedDatasetSubtreeRoot() (rfs *zfs.DatasetPath, ok bool,
) {
	sink, ok := j.mode.(*modeSink)
	if !ok {
		// make sure we didn't introduce a new job type
		_ = j.mode.(*modeSource)
		return nil, false
	}
	return sink.receiverConfig.RootWithoutClientComponent.Copy(), true
}

func (j *PassiveSide) SenderConfig() *endpoint.SenderConfig {
	source, ok := j.mode.(*modeSource)
	if !ok {
		// make sure we didn't introduce a new job type
		_ = j.mode.(*modeSink)
		return nil
	}
	return source.senderConfig
}

func (j *PassiveSide) Endpoint(clientIdentity string) Endpoint {
	return j.mode.Endpoint(clientIdentity)
}

func (*PassiveSide) RegisterMetrics(registerer prometheus.Registerer) {}

func (j *PassiveSide) Run(ctx context.Context, cron *cron.Cron) error {
	if !j.mode.Periodic() {
		return fmt.Errorf("running not periodic job: %s", j.Name())
	}

	j.mode.RunPeriodic(ctx, cron)
	GetLogger(ctx).Info("job exiting")
	return nil
}

func (j *PassiveSide) Shutdown() { j.mode.Shutdown() }
