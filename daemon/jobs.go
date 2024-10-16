package daemon

import (
	"context"
	"fmt"
	"strings"

	"github.com/dsh2dsh/cron/v3"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	"github.com/dsh2dsh/zrepl/daemon/job"
	"github.com/dsh2dsh/zrepl/daemon/job/reset"
	"github.com/dsh2dsh/zrepl/daemon/job/wakeup"
	"github.com/dsh2dsh/zrepl/daemon/logging"
	"github.com/dsh2dsh/zrepl/logger"
	"github.com/dsh2dsh/zrepl/zfs/zfscmd"
)

func newJobs(ctx context.Context, cancel context.CancelFunc) *jobs {
	g, ctx := errgroup.WithContext(ctx)
	return &jobs{
		g:   g,
		ctx: ctx,

		log:  logging.FromContext(ctx),
		cron: newCron(ctx, true),

		wakeups: make(map[string]wakeup.Func),
		resets:  make(map[string]reset.Func),

		jobs:         make(map[string]job.Job, 2),
		internalJobs: make([]job.Internal, 0, 1),

		cancel: cancel,
	}
}

type jobs struct {
	g   *errgroup.Group
	ctx context.Context

	cron *cron.Cron
	log  logger.Logger

	wakeups map[string]wakeup.Func // by Job.Name
	resets  map[string]reset.Func  // by Job.Name

	jobs         map[string]job.Job
	internalJobs []job.Internal

	cancel context.CancelFunc
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
		ret[name] = j.Status()
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

func (self *jobs) startJobsWithCron(confJobs []job.Job) {
	self.cron.Start()
	log := job.GetLogger(self.ctx)
	for _, j := range confJobs {
		if self.ctx.Err() != nil {
			self.log.WithError(context.Cause(self.ctx)).
				WithField("next_job", j.Name()).
				Error("break starting jobs")
			break
		}
		jobName := j.Name()
		if internalJobName(jobName) {
			panic("internal job name used for non-internal job " + jobName)
		} else if _, ok := self.jobs[jobName]; ok {
			panic("duplicate job name " + jobName)
		}
		self.start(self.withJobSignals(jobName), j,
			log.WithField(logging.JobField, jobName))
		self.jobs[jobName] = j
	}
	self.log.
		WithField("count", len(self.jobs)).
		WithField("internal", len(self.internalJobs)).
		Info("started jobs")
}

func internalJobName(s string) bool { return strings.HasPrefix(s, "_") }

func (self *jobs) start(ctx context.Context, j job.Internal, log logger.Logger,
) {
	j.RegisterMetrics(prometheus.DefaultRegisterer)
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
	ctx, resetFunc := reset.Context(ctx)
	self.resets[jobName] = resetFunc
	return ctx
}

func (self *jobs) context(jobName string) context.Context {
	ctx := logging.WithField(self.ctx, logging.JobField, jobName)
	ctx = zfscmd.WithJobID(ctx, jobName)
	return ctx
}

func (self *jobs) startInternal(j job.Internal) {
	log := job.GetLogger(self.ctx)
	self.start(self.ctx, j, log.WithField("server", true))
	self.internalJobs = append(self.internalJobs, j)
}
