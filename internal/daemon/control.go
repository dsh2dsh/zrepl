package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/dsh2dsh/zrepl/internal/daemon/logging"
	"github.com/dsh2dsh/zrepl/internal/daemon/middleware"
	"github.com/dsh2dsh/zrepl/internal/version"
	"github.com/dsh2dsh/zrepl/internal/zfs/zfscmd"
)

const (
	ControlJobEndpointSignal  = "/signal"
	ControlJobEndpointStatus  = "/status"
	ControlJobEndpointVersion = "/version"
)

func newControlJob(jobs *jobs) *controlJob {
	return &controlJob{jobs: jobs}
}

type controlJob struct {
	jobs *jobs
}

func (j *controlJob) Endpoints(mux *http.ServeMux, m ...middleware.Middleware,
) {
	mux.Handle(ControlJobEndpointVersion, middleware.Append(m,
		middleware.JsonResponder(j.version)))

	mux.Handle(ControlJobEndpointStatus, middleware.Append(m,
		middleware.JsonResponder(j.status)))

	mux.Handle(ControlJobEndpointSignal, middleware.Append(m,
		middleware.JsonRequestResponder(j.signal)))
}

func (j *controlJob) version(_ context.Context) (
	*version.ZreplVersionInformation, error,
) {
	v := version.NewZreplVersionInformation()
	return &v, nil
}

func (j *controlJob) status(_ context.Context) (*Status, error) {
	s := &Status{
		Jobs: j.jobs.status(),
		Global: GlobalStatus{
			ZFSCmds:   zfscmd.GetReport(),
			OsEnviron: os.Environ(),
		},
	}
	return s, nil
}

type signalRequest struct {
	Op   string
	Name string
}

func (j *controlJob) signal(ctx context.Context, req *signalRequest,
) (*struct{}, error) {
	log := logging.FromContext(ctx).With(slog.String("op", req.Op))
	if req.Name != "" {
		log = log.With(slog.String("name", req.Name))
	}
	log.Info("got signal")

	var err error
	switch req.Op {
	case "reload":
		j.jobs.Reload()
	case "reset":
		err = j.jobs.reset(req.Name)
	case "shutdown":
		j.jobs.Shutdown()
	case "stop":
		j.jobs.Cancel()
	case "wakeup":
		err = j.jobs.wakeup(req.Name)
	default:
		err = fmt.Errorf("invalid operation %q", req.Op)
	}
	return nil, err
}
