package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/dsh2dsh/cron/v3"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/dsh2dsh/zrepl/daemon/job"
	"github.com/dsh2dsh/zrepl/daemon/nethelpers"
	"github.com/dsh2dsh/zrepl/endpoint"
	"github.com/dsh2dsh/zrepl/logger"
	"github.com/dsh2dsh/zrepl/util/envconst"
	"github.com/dsh2dsh/zrepl/version"
	"github.com/dsh2dsh/zrepl/zfs"
	"github.com/dsh2dsh/zrepl/zfs/zfscmd"
)

type controlJob struct {
	sockaddr *net.UnixAddr
	sockmode os.FileMode
	jobs     *jobs
	shutdown context.CancelFunc
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

func (j *controlJob) Status() *job.Status { return &job.Status{Type: job.TypeInternal} }

func (j *controlJob) OwnedDatasetSubtreeRoot() (p *zfs.DatasetPath, ok bool) { return nil, false }

func (j *controlJob) SenderConfig() *endpoint.SenderConfig { return nil }

var promControl struct {
	requestBegin    *prometheus.CounterVec
	requestFinished *prometheus.HistogramVec
}

func (j *controlJob) RegisterMetrics(registerer prometheus.Registerer) {
	promControl.requestBegin = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "zrepl",
		Subsystem: "control",
		Name:      "request_begin",
		Help:      "number of request we started to handle",
	}, []string{"endpoint"})

	promControl.requestFinished = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "zrepl",
		Subsystem: "control",
		Name:      "request_finished",
		Help:      "time it took a request to finish",
		Buckets:   []float64{1e-6, 10e-6, 100e-6, 500e-6, 1e-3, 10e-3, 100e-3, 200e-3, 400e-3, 800e-3, 1, 10, 20},
	}, []string{"endpoint"})
	registerer.MustRegister(promControl.requestBegin)
	registerer.MustRegister(promControl.requestFinished)
}

const (
	ControlJobEndpointPProf   string = "/debug/pprof"
	ControlJobEndpointVersion string = "/version"
	ControlJobEndpointStatus  string = "/status"
	ControlJobEndpointSignal  string = "/signal"
)

func (j *controlJob) Run(ctx context.Context, cron *cron.Cron) {
	ctx, j.shutdown = context.WithCancel(ctx)
	defer j.shutdown()

	log := job.GetLogger(ctx)
	defer log.Info("control job finished")

	l, err := nethelpers.ListenUnixPrivate(j.sockaddr)
	if err != nil {
		log.WithError(err).Error("error listening")
		return
	}

	if j.sockmode != 0 {
		if err := os.Chmod(j.sockaddr.String(), j.sockmode); err != nil {
			err = fmt.Errorf("controlJob: change socket mode to %O: %w",
				j.sockmode, err)
			log.WithError(err).Error("error run control job")
			return
		}
	}

	pprofServer := NewPProfServer(ctx)
	if listen := envconst.String("ZREPL_DAEMON_AUTOSTART_PPROF_SERVER", ""); listen != "" {
		pprofServer.Control(PprofServerControlMsg{
			Run:               true,
			HttpListenAddress: listen,
		})
	}

	mux := http.NewServeMux()
	mux.Handle(ControlJobEndpointPProf,
		requestLogger{log: log, handler: jsonRequestResponder{log, func(decoder jsonDecoder) (interface{}, error) {
			var msg PprofServerControlMsg
			err := decoder(&msg)
			if err != nil {
				return nil, errors.New("decode failed")
			}
			pprofServer.Control(msg)
			return struct{}{}, nil
		}}})

	mux.Handle(ControlJobEndpointVersion,
		requestLogger{log: log, handler: jsonResponder{log, func() (interface{}, error) {
			return version.NewZreplVersionInformation(), nil
		}}})

	mux.Handle(ControlJobEndpointStatus,
		// don't log requests to status endpoint, too spammy
		jsonResponder{log, func() (interface{}, error) {
			jobs := j.jobs.status()
			globalZFS := zfscmd.GetReport()
			envconstReport := envconst.GetReport()
			s := Status{
				Jobs: jobs,
				Global: GlobalStatus{
					ZFSCmds:   globalZFS,
					Envconst:  envconstReport,
					OsEnviron: os.Environ(),
				},
			}
			return s, nil
		}})

	mux.Handle(ControlJobEndpointSignal, requestLogger{
		log: log,
		handler: jsonRequestResponder{
			log: log,
			producer: func(decoder jsonDecoder) (any, error) {
				req := struct {
					Op   string
					Name string
				}{}
				if decoder(&req) != nil {
					return nil, errors.New("decode failed")
				}

				l := log.WithField("op", req.Op)
				if req.Name != "" {
					l.WithField("name", req.Name)
				}
				l.Info("got signal")

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
					err = fmt.Errorf("operation %q is invalid", req.Op)
				}
				return struct{}{}, err
			},
		},
	})

	server := http.Server{
		Handler: mux,
		// control socket is local, 1s timeout should be more than sufficient, even on a loaded system
		WriteTimeout: envconst.Duration("ZREPL_DAEMON_CONTROL_SERVER_WRITE_TIMEOUT", 1*time.Second),
		ReadTimeout:  envconst.Duration("ZREPL_DAEMON_CONTROL_SERVER_READ_TIMEOUT", 1*time.Second),
	}

outer:
	for {
		served := make(chan error)
		go func() {
			served <- server.Serve(l)
			close(served)
		}()

		select {
		case <-ctx.Done():
			log.WithError(ctx.Err()).Info("context done")
			err := server.Shutdown(context.Background())
			if err != nil {
				log.WithError(err).Error("cannot shutdown server")
			}
			break outer
		case err = <-served:
			if err != nil {
				log.WithError(err).Error("error serving")
				break outer
			}
		}
	}

	log.Info("waiting for pprof server exit")
	pprofServer.Wait()
}

type jsonResponder struct {
	log      Logger
	producer func() (any, error)
}

func (j jsonResponder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	res, err := j.producer()
	if err != nil {
		j.writeError(err, w, "control handler error")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(res); err != nil {
		j.writeError(err, w, "control handler json marshal error")
	}
}

func (j jsonResponder) writeError(err error, w http.ResponseWriter, msg string) {
	j.log.WithError(err).Error(msg)
	w.WriteHeader(http.StatusInternalServerError)
	if _, err = io.WriteString(w, err.Error()); err != nil {
		j.log.WithError(err).Error("control handler io error")
	}
}

type jsonDecoder = func(any) error

type jsonRequestResponder struct {
	log      Logger
	producer func(decoder jsonDecoder) (any, error)
}

func (j jsonRequestResponder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var decodeErr error
	resp, err := j.producer(func(req any) error {
		decodeErr = json.NewDecoder(r.Body).Decode(&req)
		return decodeErr
	})

	// If we had a decode error ignore output of producer and return error
	if decodeErr != nil {
		j.writeError(decodeErr, w, "control handler json unmarshal error",
			http.StatusBadRequest)
		return
	} else if err != nil {
		j.writeError(err, w, "control handler error",
			http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		j.writeError(err, w, "control handler json marshal error",
			http.StatusInternalServerError)
	}
}

func (j jsonRequestResponder) writeError(err error, w http.ResponseWriter,
	msg string, statusCode int,
) {
	j.log.WithError(err).Error(msg)
	w.WriteHeader(statusCode)
	if _, err = io.WriteString(w, err.Error()); err != nil {
		j.log.WithError(err).Error("control handler io error")
	}
}

type requestLogger struct {
	log         logger.Logger
	handler     http.Handler
	handlerFunc http.HandlerFunc
}

func (l requestLogger) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log := l.log.WithField("method", r.Method).WithField("url", r.URL)
	log.Info("http request")

	promControl.requestBegin.WithLabelValues(r.URL.Path).Inc()
	defer prometheus.
		NewTimer(promControl.requestFinished.WithLabelValues(r.URL.Path)).
		ObserveDuration()

	if l.handlerFunc != nil {
		l.handlerFunc(w, r)
	} else if l.handler != nil {
		l.handler.ServeHTTP(w, r)
	} else {
		log.Error("no handler or handlerFunc configured")
	}
	log.Debug("finish")
}

func (j *controlJob) Shutdown() { j.shutdown() }
