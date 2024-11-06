package daemon

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/daemon/job"
	"github.com/dsh2dsh/zrepl/internal/daemon/logging"
	"github.com/dsh2dsh/zrepl/internal/logger"
	"github.com/dsh2dsh/zrepl/internal/version"
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
		return fmt.Errorf("daemon: cannot build logging from config: %w", err)
	}

	confJobs, connector, err := job.JobsFromConfig(conf)
	if err != nil {
		return fmt.Errorf("daemon: cannot build jobs from config: %w", err)
	}

	log := logger.NewLogger(outlets, 1*time.Second)
	log.Info(version.NewZreplVersionInformation().String())
	ctx = logging.WithLogger(ctx, log)

	log.Info("starting daemon")
	jobs := newJobs(ctx, cancel)
	// start regular jobs
	jobs.startCronJobs(confJobs)
	if err := startServer(log, conf, jobs, outlets, connector); err != nil {
		return fmt.Errorf("daemon: %w", err)
	}

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

func startServer(log logger.Logger, conf *config.Config, jobs *jobs,
	logOutlets *logger.Outlets, connecter *job.Connecter,
) error {
	server := newServerJob(log,
		newControlJob(jobs),
		newZfsJob(connecter, conf.Keys).WithTimeout(conf.Global.RpcTimeout))

	var hasControl, hasMetrics bool
	for i := range conf.Listen {
		listen := &conf.Listen[i]
		if err := server.AddServer(listen); err != nil {
			return fmt.Errorf("add server from listen[%d]: %w", i, err)
		}
		hasControl = hasControl || listen.Control
		hasMetrics = hasMetrics || listen.Metrics
	}

	if err := defaultControl(hasControl, server, conf); err != nil {
		return err
	}

	if has, err := defaultMetrics(hasMetrics, server, conf); err != nil {
		return err
	} else if has {
		logOutlets.Add(newPrometheusLogOutlet(), logger.Debug)
	}

	log.Info("starting server")
	jobs.startInternal(server)
	return nil
}

func defaultControl(exists bool, api *serverJob, conf *config.Config) error {
	if exists {
		return nil
	}

	listen := config.Listen{
		Unix:     conf.Global.Control.SockPath,
		UnixMode: conf.Global.Control.SockMode,
		Control:  true,
	}

	if err := api.AddServer(&listen); err != nil {
		return fmt.Errorf("add default control server: %w", err)
	}
	return nil
}

func defaultMetrics(exists bool, api *serverJob, conf *config.Config,
) (bool, error) {
	if exists {
		return exists, nil
	}

	for i := range conf.Global.Monitoring {
		item := &conf.Global.Monitoring[i]
		listen := config.Listen{Addr: item.Listen, Metrics: true}
		if err := api.AddServer(&listen); err != nil {
			return false, fmt.Errorf(
				"add metrics from global.monitoring[%d]: %w", i, err)
		}
		exists = true
	}
	return exists, nil
}
