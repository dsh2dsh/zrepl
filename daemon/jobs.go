package daemon

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/dsh2dsh/cron/v3"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/dsh2dsh/zrepl/daemon/job"
	"github.com/dsh2dsh/zrepl/daemon/job/reset"
	"github.com/dsh2dsh/zrepl/daemon/job/wakeup"
	"github.com/dsh2dsh/zrepl/daemon/logging"
	"github.com/dsh2dsh/zrepl/logger"
	"github.com/dsh2dsh/zrepl/zfs/zfscmd"
)

func newJobs(ctx context.Context, log logger.Logger,
	cancel context.CancelFunc,
) *jobs {
	return &jobs{
		log:  log,
		cron: newCron(logging.GetLogger(ctx, logging.SubsysCron), true),

		wakeups: make(map[string]wakeup.Func),
		resets:  make(map[string]reset.Func),

		jobs:         make(map[string]job.Job, 2),
		internalJobs: make([]job.Internal, 0, 1),

		cancel: cancel,
	}
}

type jobs struct {
	wg   sync.WaitGroup
	cron *cron.Cron
	log  logger.Logger

	wakeups map[string]wakeup.Func // by Job.Name
	resets  map[string]reset.Func  // by Job.Name

	jobs         map[string]job.Job
	internalJobs []job.Internal

	cancel context.CancelFunc
}

func (self *jobs) Cancel() {
	self.log.Info("cancel all jobs")
	self.cancel()
}

func (self *jobs) wait() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		self.wg.Wait()
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
		return fmt.Errorf("Job %q does not exist", job)
	}
	return wu()
}

func (self *jobs) startJobsWithCron(ctx context.Context, confJobs []job.Job) {
	self.cron.Start()
	log := job.GetLogger(ctx)
	for _, j := range confJobs {
		jobName := j.Name()
		if internalJobName(jobName) {
			panic("internal job name used for non-internal job " + jobName)
		} else if _, ok := self.jobs[jobName]; ok {
			panic("duplicate job name " + jobName)
		}
		self.start(self.withJobSignals(ctx, jobName), j,
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
	self.wg.Add(1)
	go func() {
		log.Info("starting job")
		j.Run(ctx, self.cron)
		log.Info("job exited")
		self.wg.Done()
	}()
}

func (self *jobs) withJobSignals(ctx context.Context, jobName string,
) context.Context {
	ctx = self.context(ctx, jobName)
	ctx, wakeup := wakeup.Context(ctx)
	self.wakeups[jobName] = wakeup
	ctx, resetFunc := reset.Context(ctx)
	self.resets[jobName] = resetFunc
	return ctx
}

func (self *jobs) context(ctx context.Context, jobName string) context.Context {
	ctx = logging.WithInjectedField(ctx, logging.JobField, jobName)
	ctx = zfscmd.WithJobID(ctx, jobName)
	return ctx
}

func (self *jobs) startInternal(ctx context.Context, j job.Internal) {
	log := job.GetLogger(ctx)
	self.start(ctx, j, log.WithField("internal", true))
	self.internalJobs = append(self.internalJobs, j)
}
