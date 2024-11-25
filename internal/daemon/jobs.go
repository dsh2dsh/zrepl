package daemon

import (
	"context"
	"fmt"
	"strings"

	"github.com/dsh2dsh/cron/v3"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	"github.com/dsh2dsh/zrepl/internal/daemon/job"
	"github.com/dsh2dsh/zrepl/internal/daemon/job/wakeup"
	"github.com/dsh2dsh/zrepl/internal/daemon/logging"
	"github.com/dsh2dsh/zrepl/internal/logger"
	"github.com/dsh2dsh/zrepl/internal/zfs/zfscmd"
)

func newJobs(ctx context.Context, cancel context.CancelFunc) *jobs {
	g, ctx := errgroup.WithContext(ctx)
	return &jobs{
		g:   g,
		ctx: ctx,

		log:  logging.FromContext(ctx),
		cron: newCron(ctx, true),

		wakeups: make(map[string]wakeup.Func),
		resets:  make(map[string]func() error),

		jobs:         make(map[string]job.Job, 2),
		internalJobs: make([]job.Internal, 0, 1),
		reloaders:    make([]func(), 0, 1),

		cancel: cancel,
	}
}

type jobs struct {
	g   *errgroup.Group
	ctx context.Context

	cron *cron.Cron
	log  *logger.Logger

	wakeups map[string]wakeup.Func  // by Job.Name
	resets  map[string]func() error // by Job.Name

	jobs         map[string]job.Job
	internalJobs []job.Internal
	reloaders    []func()

	cancel context.CancelFunc
}

func (self *jobs) Job(name string) job.Job {
	return self.jobs[name]
}

func (self *jobs) Cancel() {
	self.log.Info("stop all jobs")
	self.cancel()
}

func (self *jobs) wait() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		if err := self.g.Wait(); err != nil {
			self.log.WithError(err).Error("some jobs finished with error")
		}
		self.log.Info("all jobs finished")
		self.log.Info("waiting for cron exit")
		<-self.cron.Stop().Done()
		self.log.Info("cron exited")
		cancel()
	}()
	return ctx
}

func (self *jobs) Shutdown() {
	self.log.Info("shutdown all jobs")
	self.cron.Stop()
	for _, j := range self.jobs {
		j.Shutdown()
	}
	for _, j := range self.internalJobs {
		j.Shutdown()
	}
}

func (self *jobs) status() map[string]*job.Status {
	ret := make(map[string]*job.Status, len(self.jobs))
	for name, j := range self.jobs {
		if st := j.Status(); st != nil {
			ret[name] = st
		}
	}
	return ret
}

func (self *jobs) wakeup(job string) error {
	wu, ok := self.wakeups[job]
	if !ok {
		return fmt.Errorf("Job %q does not exist", job)
	}
	return wu()
}

func (self *jobs) reset(job string) error {
	wu, ok := self.resets[job]
	if !ok {
		return fmt.Errorf("job %q does not exist", job)
	}
	return wu()
}

func (self *jobs) startCronJobs(confJobs []job.Job) {
	log := job.GetLogger(self.ctx)
	for _, j := range confJobs {
		jobName := j.Name()
		self.mustCheckJobName(jobName)
		if self.ctx.Err() != nil {
			self.log.WithError(context.Cause(self.ctx)).
				WithField("next_job", jobName).
				Error("break starting jobs")
			break
		}
		self.jobs[jobName] = j
		j.RegisterMetrics(prometheus.DefaultRegisterer)
		log := log.WithField(logging.JobField, jobName)
		if j.Runnable() {
			self.start(self.withJobSignals(jobName), j, log)
		} else {
			log.WithField("runnable", false).Info("job initialized")
		}
	}
	self.cron.Start()
	self.log.WithField("count", len(self.jobs)).Info("started jobs")
}

func (self *jobs) mustCheckJobName(s string) {
	if strings.HasPrefix(s, "_") {
		panic("internal job name used for non-internal job " + s)
	}
	if _, ok := self.jobs[s]; ok {
		panic("duplicate job name " + s)
	}
}

func (self *jobs) start(ctx context.Context, j job.Internal, log *logger.Logger,
) {
	self.g.Go(func() error {
		log.Info("starting job")
		if err := j.Run(ctx, self.cron); err != nil {
			log.WithError(err).Error("job exited with error")
			return err
		}
		log.Info("job exited")
		return nil
	})
}

func (self *jobs) withJobSignals(jobName string) context.Context {
	ctx := self.context(jobName)
	ctx, wakeup := wakeup.Context(ctx)
	self.wakeups[jobName] = wakeup
	ctx, resetFunc := job.ContextWithReset(ctx)
	self.resets[jobName] = resetFunc
	return ctx
}

func (self *jobs) context(jobName string) context.Context {
	ctx := logging.WithField(self.ctx, logging.JobField, jobName)
	ctx = zfscmd.WithJobID(ctx, jobName)
	return ctx
}

func (self *jobs) startInternal(j job.Internal) {
	j.RegisterMetrics(prometheus.DefaultRegisterer)
	log := job.GetLogger(self.ctx)
	self.start(self.ctx, j, log.WithField("server", true))
	self.internalJobs = append(self.internalJobs, j)
}

func (self *jobs) Reload() {
	self.log.Info("reloading")
	for _, fn := range self.reloaders {
		fn()
	}
	self.log.Info("reloaded")
}

func (self *jobs) OnReload(fn func()) {
	self.reloaders = append(self.reloaders, fn)
}
