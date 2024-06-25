package daemon

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dsh2dsh/cron/v3"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/dsh2dsh/zrepl/config"
	"github.com/dsh2dsh/zrepl/daemon/job"
	"github.com/dsh2dsh/zrepl/daemon/job/reset"
	"github.com/dsh2dsh/zrepl/daemon/job/wakeup"
	"github.com/dsh2dsh/zrepl/daemon/logging"
	"github.com/dsh2dsh/zrepl/daemon/logging/trace"
	"github.com/dsh2dsh/zrepl/endpoint"
	"github.com/dsh2dsh/zrepl/logger"
	"github.com/dsh2dsh/zrepl/util/envconst"
	"github.com/dsh2dsh/zrepl/version"
	"github.com/dsh2dsh/zrepl/zfs/zfscmd"
)

func Run(ctx context.Context, conf *config.Config) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	outlets, err := logging.OutletsFromConfig(conf.Global.Logging)
	if err != nil {
		return fmt.Errorf("cannot build logging from config: %w", err)
	}
	outlets.Add(newPrometheusLogOutlet(), logger.Debug)

	confJobs, err := job.JobsFromConfig(conf, config.ParseFlagsNone)
	if err != nil {
		return fmt.Errorf("cannot build jobs from config: %w", err)
	}

	log := logger.NewLogger(outlets, 1*time.Second)
	log.Info(version.NewZreplVersionInformation().String())

	ctx = logging.WithLoggers(ctx, logging.SubsystemLoggersWithUniversalLogger(log))
	trace.RegisterCallback(trace.Callback{
		OnBegin: func(ctx context.Context) { logging.GetLogger(ctx, logging.SubsysTraceData).Debug("begin span") },
		OnEnd: func(ctx context.Context, spanInfo trace.SpanInfo) {
			logging.
				GetLogger(ctx, logging.SubsysTraceData).
				WithField("duration_s", spanInfo.EndedAt().Sub(spanInfo.StartedAt()).Seconds()).
				Debug("finished span " + spanInfo.TaskAndSpanStack(trace.SpanStackKindAnnotation))
		},
	})

	for _, job := range confJobs {
		if IsInternalJobName(job.Name()) {
			panic(fmt.Sprintf("internal job name used for config job '%s'", job.Name())) // FIXME
		}
	}

	jobs := newJobs(ctx, log, cancel)

	// start control socket
	controlJob, err := newControlJob(conf.Global.Control.SockPath, jobs)
	if err != nil {
		panic(err) // FIXME
	}
	jobs.start(ctx, controlJob, true)

	for i, jc := range conf.Global.Monitoring {
		var (
			job job.Job
			err error
		)
		switch v := jc.Ret.(type) {
		case *config.PrometheusMonitoring:
			job, err = newPrometheusJobFromConfig(v)
		default:
			return fmt.Errorf("unknown monitoring job #%d (type %T)", i, v)
		}
		if err != nil {
			return fmt.Errorf("cannot build monitoring job #%d: %w", i, err)
		}
		jobs.start(ctx, job, true)
	}

	// register global (=non job-local) metrics
	version.PrometheusRegister(prometheus.DefaultRegisterer)
	zfscmd.RegisterMetrics(prometheus.DefaultRegisterer)
	trace.RegisterMetrics(prometheus.DefaultRegisterer)
	endpoint.RegisterMetrics(prometheus.DefaultRegisterer)

	log.Info("starting daemon")

	// start regular jobs
	jobs.startJobsWithCron(ctx, confJobs, false)

	wait := jobs.wait()
	select {
	case <-wait.Done():
		log.Info("all jobs finished")
	case <-ctx.Done():
		log.WithError(ctx.Err()).Info("context finished")
	}
	log.Info("waiting for jobs to finish")
	<-wait.Done()
	log.Info("daemon exiting")
	return nil
}

type jobs struct {
	wg   sync.WaitGroup
	cron *cron.Cron
	log  logger.Logger

	// m protects all fields below it
	m       sync.RWMutex
	wakeups map[string]wakeup.Func // by Job.Name
	resets  map[string]reset.Func  // by Job.Name
	jobs    map[string]job.Job

	cancel context.CancelFunc
}

func newJobs(ctx context.Context, log logger.Logger,
	cancel context.CancelFunc,
) *jobs {
	return &jobs{
		log:     log,
		cron:    newCron(logging.GetLogger(ctx, logging.SubsysCron), true),
		wakeups: make(map[string]wakeup.Func),
		resets:  make(map[string]reset.Func),
		jobs:    make(map[string]job.Job),

		cancel: cancel,
	}
}

func (s *jobs) Cancel() {
	s.log.Info("cancel all jobs")
	s.cancel()
}

func (s *jobs) wait() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		s.wg.Wait()
		<-s.cron.Stop().Done()
		cancel()
	}()
	return ctx
}

func (s *jobs) Shutdown() {
	s.log.Info("shutdown all jobs")
	s.cron.Stop()
	for _, j := range s.jobs {
		j.Shutdown()
	}
}

type Status struct {
	Jobs   map[string]*job.Status
	Global GlobalStatus
}

type GlobalStatus struct {
	ZFSCmds   *zfscmd.Report
	Envconst  *envconst.Report
	OsEnviron []string
}

func (s *jobs) status() map[string]*job.Status {
	s.m.RLock()
	defer s.m.RUnlock()

	ret := make(map[string]*job.Status, len(s.jobs))
	for name, j := range s.jobs {
		ret[name] = j.Status()
	}
	return ret
}

func (s *jobs) wakeup(job string) error {
	s.m.RLock()
	defer s.m.RUnlock()

	wu, ok := s.wakeups[job]
	if !ok {
		return fmt.Errorf("Job %s does not exist", job)
	}
	return wu()
}

func (s *jobs) reset(job string) error {
	s.m.RLock()
	defer s.m.RUnlock()

	wu, ok := s.resets[job]
	if !ok {
		return fmt.Errorf("Job %s does not exist", job)
	}
	return wu()
}

func (s *jobs) startJobsWithCron(ctx context.Context, confJobs []job.Job,
	internal bool,
) {
	s.cron.Start()
	for _, j := range confJobs {
		s.start(ctx, j, internal)
	}
}

const (
	jobNamePrometheus = "_prometheus"
	jobNameControl    = "_control"
)

func IsInternalJobName(s string) bool {
	return strings.HasPrefix(s, "_")
}

func (s *jobs) start(ctx context.Context, j job.Job, internal bool) {
	s.m.Lock()
	defer s.m.Unlock()

	jobName := j.Name()
	if !internal && IsInternalJobName(jobName) {
		panic(fmt.Sprintf(
			"internal job name used for non-internal job %s", jobName))
	} else if internal && !IsInternalJobName(jobName) {
		panic(fmt.Sprintf(
			"internal job does not use internal job name %s", jobName))
	} else if _, ok := s.jobs[jobName]; ok {
		panic(fmt.Sprintf("duplicate job name %s", jobName))
	}

	j.RegisterMetrics(prometheus.DefaultRegisterer)
	s.jobs[jobName] = j

	ctx = logging.WithInjectedField(ctx, logging.JobField, j.Name())
	ctx = zfscmd.WithJobID(ctx, j.Name())
	ctx, wakeup := wakeup.Context(ctx)
	ctx, resetFunc := reset.Context(ctx)
	s.wakeups[jobName] = wakeup
	s.resets[jobName] = resetFunc

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		job.GetLogger(ctx).Info("starting job")
		defer job.GetLogger(ctx).Info("job exited")
		j.Run(ctx, s.cron)
	}()
}
