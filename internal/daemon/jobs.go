package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/dsh2dsh/cron/v3"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	"github.com/dsh2dsh/zrepl/internal/daemon/job"
	"github.com/dsh2dsh/zrepl/internal/daemon/job/signal"
	"github.com/dsh2dsh/zrepl/internal/daemon/logging"
	"github.com/dsh2dsh/zrepl/internal/logger"
	"github.com/dsh2dsh/zrepl/internal/zfs/zfscmd"
)

func newJobs(ctx context.Context, cancel context.CancelFunc) *jobs {
	g, ctx := errgroup.WithContext(ctx)
	graceful, gracefulStop := context.WithCancelCause(ctx)

	return &jobs{
		g:      g,
		ctx:    ctx,
		cancel: cancel,

		graceful:     graceful,
		gracefulStop: gracefulStop,

		log:  logging.FromContext(ctx),
		cron: newCron(ctx, true),

		jobs:         make(map[string]*props, 2),
		internalJobs: make([]job.Internal, 0, 1),
		reloaders:    make([]func(), 0, 1),
	}
}

type jobs struct {
	g      *errgroup.Group
	ctx    context.Context
	cancel context.CancelFunc

	graceful     context.Context
	gracefulStop context.CancelCauseFunc

	cron *cron.Cron
	log  *slog.Logger

	jobs         map[string]*props
	internalJobs []job.Internal
	reloaders    []func()
}

type props struct {
	job    job.Job
	cronId cron.EntryID

	mu     sync.Mutex
	wakeup context.CancelCauseFunc
	reset  context.CancelCauseFunc

	wakeupBusy int
	err        error
}

func (self *props) Context(ctx context.Context) context.Context {
	wakeup, wakeupStop := context.WithCancelCause(context.Background())
	ctx = signal.WithWakeup(ctx, wakeup)
	self.mu.Lock()
	defer self.mu.Unlock()
	self.wakeup = wakeupStop
	ctx, self.reset = context.WithCancelCause(ctx)
	return ctx
}

func (self *props) Stop() {
	self.mu.Lock()
	defer self.mu.Unlock()
	self.wakeup(nil)
	self.wakeup = nil
	self.reset(nil)
	self.reset = nil
}

func (self *props) Wakeup(cause error, wakeupBusy bool) bool {
	self.mu.Lock()
	defer self.mu.Unlock()
	if !self.running() {
		return false
	} else if wakeupBusy {
		self.wakeupBusy++
		self.err = fmt.Errorf("job frequency is too high; was skipped %d times",
			self.wakeupBusy)
	}
	self.wakeup(cause)
	return true
}

func (self *props) running() bool { return self.reset != nil }

func (self *props) Reset(cause error) bool {
	self.mu.Lock()
	defer self.mu.Unlock()
	if !self.running() {
		return false
	}
	self.reset(cause)
	return true
}

func (self *props) PreRun() job.Job {
	self.mu.Lock()
	defer self.mu.Unlock()
	self.err = nil
	return self.job
}

func (self *jobs) Cancel() {
	self.log.Info("stop all jobs")
	self.cancel()
}

func (self *jobs) wait() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		if err := self.g.Wait(); err != nil {
			logger.WithError(self.log, err, "some jobs finished with error")
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
	self.log.Info("graceful stop all jobs")
	self.cron.Stop()
	self.gracefulStop(errors.New("graceful stop"))
}

func (self *jobs) status() map[string]*job.Status {
	ret := make(map[string]*job.Status, len(self.jobs))
	for name, j := range self.jobs {
		if s := j.job.Status(); s != nil {
			ret[name] = self.updateStatus(j, s)
		}
	}
	return ret
}

func (self *jobs) updateStatus(j *props, s *job.Status) *job.Status {
	if errStr := s.Error(); errStr == "" && j.err != nil {
		s.Err = j.err.Error()
	}
	if j.cronId > 0 {
		entry := self.cron.Entry(j.cronId)
		s.NextCron = entry.Next
	}
	return s
}

func (self *jobs) wakeup(name string) error {
	j, ok := self.jobs[name]
	if !ok {
		return fmt.Errorf("job does not exist: %s", name)
	}

	log := job.GetLogger(self.ctx).With(
		slog.String(logging.JobField, name))
	log.Info("wakeup job from signal")
	if j.Wakeup(errors.New("wakeup from signal"), false) {
		return nil
	}

	log.Info("start job from wakeup signal")
	self.runJob(j, log)
	return nil
}

func (self *jobs) reset(name string) error {
	j, ok := self.jobs[name]
	if !ok {
		return fmt.Errorf("job does not exist: %s", name)
	} else if !j.Reset(errors.New("reset signal")) {
		return fmt.Errorf("job not running: %s", name)
	}
	return nil
}

func (self *jobs) startCronJobs(confJobs []job.Job) {
	log := job.GetLogger(self.ctx)
	var runCount int
	for _, j := range confJobs {
		name := j.Name()
		self.mustCheckJobName(name)
		if self.ctx.Err() != nil {
			logger.WithError(self.log.With(slog.String("next_job", name)),
				context.Cause(self.ctx), "break starting jobs")
			break
		}
		p := &props{job: j}
		self.jobs[name] = p
		j.RegisterMetrics(prometheus.DefaultRegisterer)
		log := log.With(slog.String(logging.JobField, name))
		if j.Runnable() {
			self.runJob(p, log)
			runCount++
		} else {
			log.With(slog.Bool("runnable", false)).Info("job initialized")
		}
		self.registerCron(p, log)
	}

	self.cron.Start()
	self.log.With(slog.Int("count", len(self.jobs)), slog.Int("run", runCount)).
		Info("started jobs")
}

func (self *jobs) mustCheckJobName(s string) {
	if strings.HasPrefix(s, "_") {
		panic("internal job name used for non-internal job " + s)
	}
	if _, ok := self.jobs[s]; ok {
		panic("duplicate job name " + s)
	}
}

func (self *jobs) runJob(p *props, log *slog.Logger) {
	fn := self.makeStartFunc(self.context(p), p.PreRun(), log)
	self.g.Go(func() error {
		defer p.Stop()
		return fn()
	})
}

func (self *jobs) makeStartFunc(ctx context.Context, j job.Internal,
	log *slog.Logger,
) func() error {
	ctx, stopGraceful := self.gracefulContext(ctx, log)
	fn := func() error {
		defer stopGraceful()
		log.Info("starting job")
		if err := j.Run(ctx); err != nil {
			logger.WithError(log, err, "job exited with error")
			return err
		}
		log.Info("job exited")
		return nil
	}
	return fn
}

func (self *jobs) gracefulContext(ctx context.Context, log *slog.Logger,
) (context.Context, func()) {
	graceful, gracefulStop := context.WithCancelCause(ctx)
	stop := context.AfterFunc(self.graceful, func() {
		log.Info("graceful stop received")
		gracefulStop(context.Cause(self.graceful))
	})
	ctx = signal.WithGraceful(ctx, graceful)

	fn := func() {
		gracefulStop(nil)
		stop()
	}
	return ctx, fn
}

func (self *jobs) context(p *props) context.Context {
	name := p.job.Name()
	ctx := logging.With(self.ctx, slog.String(logging.JobField, name))
	ctx = zfscmd.WithJobID(ctx, name)
	return p.Context(ctx)
}

func (self *jobs) registerCron(p *props, log *slog.Logger) {
	cronSpec := p.job.Cron()
	if cronSpec == "" {
		log.Info("cron skip non periodic job")
		return
	}
	log = log.With(slog.String("cron", cronSpec))

	log.Info("register cron job")
	id, err := self.cron.AddFunc(cronSpec, func() {
		self.handleCron(p, log)
	})
	if err != nil {
		logger.WithError(log, err, "failed add cron job")
	}
	p.cronId = id
}

func (self *jobs) handleCron(j *props, log *slog.Logger) {
	log.Info("start job from cron")
	if j.Wakeup(errors.New("wakeup from cron"), true) {
		log.Warn("job took longer than its interval")
		return
	}
	self.runJob(j, log)
}

func (self *jobs) startInternal(j job.Internal) {
	j.RegisterMetrics(prometheus.DefaultRegisterer)
	log := job.GetLogger(self.ctx).With(slog.Bool("internal", true))
	self.g.Go(self.makeStartFunc(self.ctx, j, log))
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
