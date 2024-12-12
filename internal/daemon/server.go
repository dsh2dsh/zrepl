package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/daemon/job"
	"github.com/dsh2dsh/zrepl/internal/daemon/job/signal"
	"github.com/dsh2dsh/zrepl/internal/daemon/middleware"
	"github.com/dsh2dsh/zrepl/internal/logger"
)

func newServerJob(log *slog.Logger, controlJob *controlJob, zfsJob *zfsJob,
) *serverJob {
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
		zfsJob:     zfsJob,
	}
	return j.init()
}

type serverJob struct {
	reqBegin    *prometheus.CounterVec
	reqFinished *prometheus.HistogramVec

	middlewares []middleware.Middleware
	prometheus  middleware.Middleware

	log     *slog.Logger
	servers []*server

	controlJob *controlJob
	hasMetrics bool
	zfsJob     *zfsJob
}

var _ job.Internal = (*serverJob)(nil)

func (self *serverJob) init() *serverJob {
	self.prometheus = middleware.PrometheusMetrics(self.reqBegin,
		self.reqFinished)
	self.middlewares = []middleware.Middleware{
		middleware.RequestLogger(
			// don't log requests to status endpoint, too spammy
			middleware.WithCustomLevel(ControlJobEndpointStatus, slog.LevelDebug),
			middleware.WithCustomLevel("/metrics", slog.LevelDebug)),
		self.prometheus,
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
	self.log.With(
		slog.String("addr", c.Addr),
		slog.String("unix", c.Unix),
		slog.Bool("control", c.Control),
		slog.Bool("metrics", c.Metrics),
		slog.Bool("zfs", c.Zfs),
	).Info("adding listener")

	s := &server{
		Server: &http.Server{
			Addr:    c.Addr,
			Handler: self.mux(c),

			ReadHeaderTimeout: 10 * time.Second,
			IdleTimeout:       30 * time.Second,
		},
		certFile: c.TLSCert,
		keyFile:  c.TLSKey,
	}

	if c.Unix != "" {
		if s.Addr != "" {
			self.servers = append(self.servers, s)
			s = s.Clone()
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
		self.controlJob.Endpoints(mux, self.middlewares...)
	}
	if c.Metrics {
		self.hasMetrics = true
		metricsEndpoints(mux, self.middlewares...)
	}
	if c.Zfs {
		self.zfsJob.Endpoints(mux, self.prometheus)
	}
	return mux
}

func (self *serverJob) Run(ctx context.Context) error {
	defer self.log.Info("server finished")
	g, ctx := errgroup.WithContext(ctx)
	baseContext := func(net.Listener) context.Context { return ctx }

	ctx, gracefulStop := context.WithCancelCause(ctx)
	defer gracefulStop(nil)
	graceful := signal.GracefulFrom(ctx)
	defer context.AfterFunc(graceful, func() {
		self.log.Info("graceful stop server")
		gracefulStop(context.Cause(graceful))
	})()

	for _, s := range self.servers {
		s.BaseContext = baseContext
		g.Go(func() error {
			self.log.With(slog.String("addr", s.Addr)).Info("listen on")
			return s.Serve()
		})
	}

	self.log.Info("waiting for listeners to finish")
	<-ctx.Done()
	self.log.With(slog.String("cause", context.Cause(ctx).Error())).
		Info("server context done")
	self.shutdownServers()

	if err := g.Wait(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.WithError(self.log, err, "error serving")
		return fmt.Errorf("daemon server: %w", err)
	}
	return nil
}

func (self *serverJob) shutdownServers() {
	for _, s := range self.servers {
		self.log.With(slog.String("addr", s.Addr)).Info("graceful stop listener")
		if err := s.Shutdown(context.Background()); err != nil {
			logger.WithError(self.log, err, "can't shutdown server")
		}
	}
}

func (self *serverJob) OnReload() { _ = self.Reload(false) }

func (self *serverJob) Reload(breakOnError bool) error {
	self.log.Info("reload all listeners")
	for _, s := range self.servers {
		l := self.log.With(slog.String("addr", s.Addr))
		l.Info("reload listener")
		if err := s.Reload(l); err != nil {
			logger.WithError(l, err, "failed reload listener")
			if breakOnError {
				return err
			}
		}
	}
	self.log.Info("all listeners reloaded")
	return nil
}
