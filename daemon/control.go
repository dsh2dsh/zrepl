package daemon

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/dsh2dsh/cron/v3"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/dsh2dsh/zrepl/daemon/logging"
	"github.com/dsh2dsh/zrepl/daemon/middleware"
	"github.com/dsh2dsh/zrepl/daemon/nethelpers"
	"github.com/dsh2dsh/zrepl/logger"
	"github.com/dsh2dsh/zrepl/util/envconst"
	"github.com/dsh2dsh/zrepl/version"
	"github.com/dsh2dsh/zrepl/zfs/zfscmd"
)

const (
	jobNameControl = "_control"

	ControlJobEndpointPProf   string = "/debug/pprof"
	ControlJobEndpointVersion string = "/version"
	ControlJobEndpointStatus  string = "/status"
	ControlJobEndpointSignal  string = "/signal"
)

type controlJob struct {
	sockaddr *net.UnixAddr
	sockmode os.FileMode
	jobs     *jobs
	shutdown context.CancelFunc

	log         logger.Logger
	pprofServer *pprofServer

	requestBegin    *prometheus.CounterVec
	requestFinished *prometheus.HistogramVec
}

func newControlJob(sockpath string, jobs *jobs, mode uint32,
) (*controlJob, error) {
	j := &controlJob{
		sockmode: os.FileMode(mode),
		jobs:     jobs,
	}

	sockaddr, err := net.ResolveUnixAddr("unix", sockpath)
	if err != nil {
		return nil, fmt.Errorf("cannot resolve unix address %q: %w", sockaddr, err)
	}
	j.sockaddr = sockaddr
	return j, nil
}

func (j *controlJob) Name() string { return jobNameControl }

func (j *controlJob) RegisterMetrics(registerer prometheus.Registerer) {
	j.requestBegin = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "zrepl",
		Subsystem: "control",
		Name:      "request_begin",
		Help:      "number of request we started to handle",
	}, []string{"endpoint"})

	j.requestFinished = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "zrepl",
		Subsystem: "control",
		Name:      "request_finished",
		Help:      "time it took a request to finish",
		Buckets:   []float64{1e-6, 10e-6, 100e-6, 500e-6, 1e-3, 10e-3, 100e-3, 200e-3, 400e-3, 800e-3, 1, 10, 20},
	}, []string{"endpoint"})
	registerer.MustRegister(j.requestBegin)
	registerer.MustRegister(j.requestFinished)
}

func (j *controlJob) Run(ctx context.Context, cron *cron.Cron) {
	ctx, j.shutdown = context.WithCancel(ctx)
	defer j.shutdown()

	j.log = logging.GetLogger(ctx, logging.SubsysInternal)
	defer j.log.Info("control job finished")

	l, err := nethelpers.ListenUnixPrivate(j.sockaddr)
	if err != nil {
		j.log.WithError(err).Error("error listening")
		return
	}

	if j.sockmode != 0 {
		if err := os.Chmod(j.sockaddr.String(), j.sockmode); err != nil {
			err = fmt.Errorf("controlJob: change socket mode to %O: %w",
				j.sockmode, err)
			j.log.WithError(err).Error("error run control job")
			return
		}
	}

	j.pprofServer = NewPProfServer(ctx)
	if listen := envconst.String(
		"ZREPL_DAEMON_AUTOSTART_PPROF_SERVER", ""); listen != "" {
		j.pprofServer.Control(PprofServerControlMsg{
			Run:               true,
			HttpListenAddress: listen,
		})
	}

	server := http.Server{
		Handler: j.mux(),
		// control socket is local, 1s timeout should be more than sufficient, even
		// on a loaded system
		WriteTimeout: envconst.Duration(
			"ZREPL_DAEMON_CONTROL_SERVER_WRITE_TIMEOUT", time.Second),
		ReadTimeout: envconst.Duration(
			"ZREPL_DAEMON_CONTROL_SERVER_READ_TIMEOUT", time.Second),
	}

	served := make(chan error)
	go func() {
		served <- server.Serve(l)
		close(served)
	}()

	select {
	case <-ctx.Done():
		j.log.WithError(ctx.Err()).Info("context done")
		if err := server.Shutdown(context.Background()); err != nil {
			j.log.WithError(err).Error("cannot shutdown server")
		}
	case err := <-served:
		if err != nil {
			j.log.WithError(err).Error("error serving")
		}
	}
	<-served

	j.log.Info("waiting for pprof server exit")
	j.pprofServer.Wait()
}

func (j *controlJob) mux() *http.ServeMux {
	logRequest := middleware.RequestLogger(j.log,
		middleware.WithPrometheusMetrics(j.requestBegin, j.requestFinished))

	mux := http.NewServeMux()
	mux.Handle(ControlJobEndpointPProf, middleware.New(
		logRequest,
		middleware.JsonRequestResponder(j.log, j.pprof)))

	mux.Handle(ControlJobEndpointVersion, middleware.New(
		logRequest,
		middleware.JsonResponder(j.log, j.version)))

	mux.Handle(ControlJobEndpointStatus, middleware.New(
		// don't log requests to status endpoint, too spammy
		middleware.JsonResponder(j.log, j.status)))

	mux.Handle(ControlJobEndpointSignal, middleware.New(
		logRequest,
		middleware.JsonRequestResponder(j.log, j.signal)))
	return mux
}

func (j *controlJob) pprof(msg *PprofServerControlMsg) (struct{}, error) {
	j.pprofServer.Control(*msg)
	return struct{}{}, nil
}

func (j *controlJob) version() (version.ZreplVersionInformation, error) {
	return version.NewZreplVersionInformation(), nil
}

func (j *controlJob) status() (Status, error) {
	s := Status{
		Jobs: j.jobs.status(),
		Global: GlobalStatus{
			ZFSCmds:   zfscmd.GetReport(),
			Envconst:  envconst.GetReport(),
			OsEnviron: os.Environ(),
		},
	}
	return s, nil
}

type signalRequest struct {
	Op   string
	Name string
}

func (j *controlJob) signal(req *signalRequest) (struct{}, error) {
	log := j.log.WithField("op", req.Op)
	if req.Name != "" {
		log.WithField("name", req.Name)
	}
	log.Info("got signal")

	var err error
	switch req.Op {
	case "wakeup":
		err = j.jobs.wakeup(req.Name)
	case "reset":
		err = j.jobs.reset(req.Name)
	case "stop":
		j.jobs.Cancel()
	case "shutdown":
		j.jobs.Shutdown()
	default:
		err = fmt.Errorf("invalid operation %q", req.Op)
	}
	return struct{}{}, err
}

func (j *controlJob) Shutdown() { j.shutdown() }
