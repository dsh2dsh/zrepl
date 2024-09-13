package daemon

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/dsh2dsh/zrepl/config"
	"github.com/dsh2dsh/zrepl/daemon/job"
	"github.com/dsh2dsh/zrepl/daemon/logging"
	"github.com/dsh2dsh/zrepl/daemon/logging/trace"
	"github.com/dsh2dsh/zrepl/endpoint"
	"github.com/dsh2dsh/zrepl/logger"
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

	// register global (=non job-local) metrics
	version.PrometheusRegister(prometheus.DefaultRegisterer)
	zfscmd.RegisterMetrics(prometheus.DefaultRegisterer)
	trace.RegisterMetrics(prometheus.DefaultRegisterer)
	endpoint.RegisterMetrics(prometheus.DefaultRegisterer)

	log.Info("starting daemon")
	jobs := newJobs(ctx, log, cancel)
	if err := startInternalJobs(ctx, conf, jobs); err != nil {
		return err
	}

	// start regular jobs
	jobs.startJobsWithCron(ctx, confJobs)

	wait := jobs.wait()
	select {
	case <-wait.Done():
	case <-ctx.Done():
		log.WithError(ctx.Err()).Info("context canceled")
	}
	log.Info("waiting for jobs to finish")
	<-wait.Done()
	log.Info("daemon exiting")
	return nil
}

func startInternalJobs(ctx context.Context, conf *config.Config, jobs *jobs,
) error {
	// start control socket
	if err := startControlJob(ctx, conf, jobs); err != nil {
		return err
	} else if err := startPrometheusJobs(ctx, conf, jobs); err != nil {
		return err
	}
	return nil
}

func startControlJob(ctx context.Context, conf *config.Config, jobs *jobs,
) error {
	j, err := newControlJob(conf.Global.Control.SockPath,
		jobs, conf.Global.Control.SockMode)
	if err != nil {
		return fmt.Errorf("starting control job: %w", err)
	}
	jobs.startInternal(ctx, j)
	return nil
}

func startPrometheusJobs(ctx context.Context, conf *config.Config, jobs *jobs,
) error {
	for i, jc := range conf.Global.Monitoring {
		v, ok := jc.Ret.(*config.PrometheusMonitoring)
		if !ok {
			return fmt.Errorf("unknown monitoring job #%d (type %T)", i, v)
		}
		j, err := newPrometheusJobFromConfig(v)
		if err != nil {
			return fmt.Errorf("cannot build monitoring job #%d: %w", i, err)
		}
		jobs.startInternal(ctx, j)
	}
	return nil
}
