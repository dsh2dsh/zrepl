package daemon

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"

	"github.com/dsh2dsh/cron/v3"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/daemon/middleware"
	"github.com/dsh2dsh/zrepl/internal/logger"
)

func newServerJob(log logger.Logger, controlJob *controlJob) *serverJob {
	j := &serverJob{
		reqBegin: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "zrepl",
			Subsystem: "control",
			Name:      "request_begin",
			Help:      "number of request we started to handle",
		}, []string{"endpoint"}),

		reqFinished: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "zrepl",
			Subsystem: "control",
			Name:      "request_finished",
			Help:      "time it took a request to finish",
			Buckets: []float64{
				1e-6, 10e-6, 100e-6, 500e-6, 1e-3, 10e-3, 100e-3, 200e-3, 400e-3, 800e-3,
				1, 10, 20,
			},
		}, []string{"endpoint"}),

		log:     log,
		servers: make([]*server, 0, 2),

		controlJob: controlJob,
	}
	return j.init()
}

type serverJob struct {
	reqBegin    *prometheus.CounterVec
	reqFinished *prometheus.HistogramVec

	log            logger.Logger
	defaultMiddles []middleware.Middleware

	servers  []*server
	shutdown context.CancelFunc

	controlJob *controlJob
	hasMetrics bool
}

func (self *serverJob) init() *serverJob {
	self.defaultMiddles = []middleware.Middleware{
		middleware.RequestLogger(
			// don't log requests to status endpoint, too spammy
			middleware.WithCustomLevel(ControlJobEndpointStatus, logger.Debug)),
		middleware.PrometheusMetrics(self.reqBegin, self.reqFinished),
	}
	return self
}

func (self *serverJob) RegisterMetrics(registerer prometheus.Registerer) {
	registerer.MustRegister(self.reqBegin, self.reqFinished)
	if self.hasMetrics {
		mustRegisterMetrics(registerer)
	}
}

func (self *serverJob) AddServer(c *config.Listen) error {
	self.log.WithField("addr", c.Addr).WithField("unix", c.Unix).
		WithField("control", c.Control).
		WithField("metrics", c.Metrics).
		Info("adding listener")

	s := &server{
		Server: &http.Server{Addr: c.Addr, Handler: self.mux(c)},
		cert:   c.TLSCert,
		key:    c.TLSKey,
	}

	if c.Unix != "" {
		if s.Addr != "" {
			self.servers = append(self.servers, s)
			cloned := *s
			s = &cloned
		}
		if err := s.WithUnix(c.Unix, c.UnixMode); err != nil {
			return fmt.Errorf("add server: %w", err)
		}
	}

	self.servers = append(self.servers, s)
	return nil
}

func (self *serverJob) mux(c *config.Listen) *http.ServeMux {
	mux := http.NewServeMux()
	if c.Control {
		self.controlJob.Endpoints(mux, self.defaultMiddles...)
	}
	if c.Metrics {
		self.hasMetrics = true
		metricsEndpoints(mux, self.defaultMiddles...)
	}
	return mux
}

func (self *serverJob) Run(ctx context.Context, cron *cron.Cron) error {
	defer self.log.Info("server finished")

	g, ctx := errgroup.WithContext(ctx)
	baseContext := func(l net.Listener) context.Context { return ctx }
	ctx, self.shutdown = context.WithCancel(ctx)

	for _, s := range self.servers {
		s.BaseContext = baseContext
		g.Go(func() error {
			self.log.WithField("addr", s.Addr).Info("listen on")
			return s.Serve()
		})
	}

	self.log.Info("waiting for listeners to finish")
	<-ctx.Done()
	self.log.WithError(context.Cause(ctx)).Info("context done")
	self.shutdownServers()
	if err := g.Wait(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		self.log.WithError(err).Error("error serving")
		return fmt.Errorf("daemon: %w", err)
	}
	return nil
}

func (self *serverJob) shutdownServers() {
	for _, s := range self.servers {
		self.log.WithField("addr", s.Addr).Info("shutdown listener")
		if err := s.Shutdown(context.Background()); err != nil {
			self.log.WithError(err).Error("can't shutdown server")
		}
	}
}

func (self *serverJob) Shutdown() {
	if self.shutdown != nil {
		self.log.Info("cancel context on shutdown")
		self.shutdown()
	}
}
