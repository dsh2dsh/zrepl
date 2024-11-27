package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/daemon/job"
	"github.com/dsh2dsh/zrepl/internal/daemon/logging"
	"github.com/dsh2dsh/zrepl/internal/logger"
	"github.com/dsh2dsh/zrepl/internal/version"
)

func Run(ctx context.Context, conf *config.Config) error {
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	outlets, err := logging.OutletsFromConfig(conf.Global.Logging)
	if err != nil {
		return fmt.Errorf("daemon: cannot build logging from config: %w", err)
	}

	confJobs, connector, err := job.JobsFromConfig(conf)
	if err != nil {
		return fmt.Errorf("daemon: cannot build jobs from config: %w", err)
	}

	log := logger.NewLogger(outlets)
	slog.SetDefault(log.Logger)
	log.Info(version.NewZreplVersionInformation().String())
	ctx = logging.WithLogger(ctx, log)

	log.Info("starting daemon")
	jobs := newJobs(ctx, cancel)
	// start regular jobs
	jobs.startCronJobs(confJobs)
	if err := startServer(ctx, conf, jobs, outlets, connector); err != nil {
		return fmt.Errorf("daemon: %w", err)
	}

	waitDone(ctx, jobs)
	return nil
}

func startServer(ctx context.Context, conf *config.Config, jobs *jobs,
	logOutlets *logger.Outlets, connecter *job.Connecter,
) error {
	log := logging.FromContext(ctx)
	server := newServerJob(log,
		newControlJob(jobs),
		newZfsJob(connecter, conf.Keys).WithTimeout(conf.Global.RpcTimeout))

	var hasControl, hasMetrics bool
	for i := range conf.Listen {
		listen := &conf.Listen[i]
		if err := server.AddServer(listen); err != nil {
			return fmt.Errorf("failed add server from listen[%d]: %w", i, err)
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
		logOutlets.Add(newPrometheusLogOutlet())
	}

	log.Info("starting server")
	if err := server.Reload(true); err != nil {
		return fmt.Errorf("failed start server: %w", err)
	}
	jobs.startInternal(server)
	jobs.OnReload(server.OnReload)
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

func waitDone(ctx context.Context, jobs *jobs) {
	sigReload := make(chan os.Signal, 1)
	signal.Notify(sigReload, syscall.SIGHUP)

	sigTerm := make(chan os.Signal, 1)
	signal.Reset(syscall.SIGTERM)
	signal.Notify(sigTerm, syscall.SIGTERM)

	log := logging.FromContext(ctx)
	log.Info("waiting for jobs to finish")
	wait := jobs.wait()

	for {
		select {
		case <-wait.Done():
			log.Info("daemon exiting")
			return
		case <-sigReload:
			log.Info("got HUP signal")
			jobs.Reload()
		case <-sigTerm:
			log.Info("got TERM signal")
			jobs.Shutdown()
		}
	}
}
