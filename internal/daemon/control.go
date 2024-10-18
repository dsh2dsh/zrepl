package daemon

import (
	"fmt"
	"net/http"
	"os"

	"github.com/dsh2dsh/zrepl/internal/daemon/middleware"
	"github.com/dsh2dsh/zrepl/internal/logger"
	"github.com/dsh2dsh/zrepl/internal/util/envconst"
	"github.com/dsh2dsh/zrepl/internal/version"
	"github.com/dsh2dsh/zrepl/internal/zfs/zfscmd"
)

const (
	ControlJobEndpointSignal  = "/signal"
	ControlJobEndpointStatus  = "/status"
	ControlJobEndpointVersion = "/version"
)

func newControlJob(jobs *jobs, log logger.Logger) *controlJob {
	return &controlJob{jobs: jobs, log: log}
}

type controlJob struct {
	jobs *jobs
	log  logger.Logger
}

func (j *controlJob) Endpoints(mux *http.ServeMux, m ...middleware.Middleware,
) {
	mux.Handle(ControlJobEndpointVersion, middleware.Append(m,
		middleware.JsonResponder(j.log, j.version)))

	mux.Handle(ControlJobEndpointStatus, middleware.Append(m,
		middleware.JsonResponder(j.log, j.status)))

	mux.Handle(ControlJobEndpointSignal, middleware.Append(m,
		middleware.JsonRequestResponder(j.log, j.signal)))
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
		log = log.WithField("name", req.Name)
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
