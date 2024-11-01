package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"time"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/daemon/job"
	"github.com/dsh2dsh/zrepl/internal/daemon/middleware"
	"github.com/dsh2dsh/zrepl/internal/replication/logic/pdu"
)

func newZfsJob(connecter *job.Connecter, keys []config.AuthKey) *zfsJob {
	j := &zfsJob{connecter: connecter, timeout: time.Minute}
	return j.init(keys)
}

type zfsJob struct {
	connecter   *job.Connecter
	middlewares []middleware.Middleware

	timeout time.Duration
}

func (self *zfsJob) init(keys []config.AuthKey) *zfsJob {
	self.middlewares = []middleware.Middleware{
		middleware.RequestId,
		middleware.RequestLogger(middleware.WithCompletedInfo()),
		middleware.ExtractJobName("job", func(name string) bool {
			return self.connecter.Job(name) != nil
		}),
		middleware.CheckClientIdentity(keys),
	}
	return self
}

func (self *zfsJob) WithTimeout(d time.Duration) *zfsJob {
	if d > 0 {
		self.timeout = d
	}
	return self
}

func (self *zfsJob) Endpoints(mux *http.ServeMux, m ...middleware.Middleware) {
	ep := job.EndpointNames("{job}")
	m = slices.Concat(m, self.middlewares)
	mux.Handle(ep[job.EpListFilesystems], middleware.Append(m,
		middleware.GzipResponse,
		middleware.JsonResponder(self.listFilesystems)))
	mux.Handle(ep[job.EpListFilesystemVersions], middleware.Append(m,
		middleware.GzipResponse,
		middleware.JsonRequestResponder(self.listFilesystemVersions)))
	mux.Handle(ep[job.EpDestroySnapshots], middleware.Append(m,
		middleware.GzipResponse,
		middleware.JsonRequestResponder(self.destroySnapshots)))
	mux.Handle(ep[job.EpWaitForConnectivity],
		middleware.AppendHandler(m, http.HandlerFunc(self.healthCheck)))

	mux.Handle(ep[job.EpReceive], middleware.Append(m,
		middleware.JsonRequestStream(self.receive)))

	mux.Handle(ep[job.EpSend], middleware.Append(m,
		middleware.JsonRequestResponseStream(self.send)))
	mux.Handle(ep[job.EpSendDry], middleware.Append(m,
		middleware.JsonRequestResponder(self.sendDry)))
	mux.Handle(ep[job.EpSendCompleted], middleware.Append(m,
		middleware.JsonRequestResponder(self.sendCompleted)))
	mux.Handle(ep[job.EpReplicationCursor], middleware.Append(m,
		middleware.JsonRequestResponder(self.replicationCursor)))
}

func (self *zfsJob) healthCheck(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (self *zfsJob) listFilesystems(ctx context.Context,
) (*pdu.ListFilesystemRes, error) {
	ep, err := self.jobEndpoint(ctx)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, self.timeout)
	defer cancel()

	var req pdu.ListFilesystemReq
	resp, err := ep.ListFilesystems(ctx, &req)
	if err != nil {
		return nil, fmt.Errorf("list filesystems: %w", err)
	}
	return resp, nil
}

func (self *zfsJob) jobEndpoint(ctx context.Context) (job.Endpoint, error) {
	jobName := middleware.JobNameFrom(ctx)
	if jobName == "" {
		return nil, errors.New("context has no job")
	}

	j := self.connecter.Job(jobName)
	if j == nil {
		return nil, fmt.Errorf("job %q not found", jobName)
	}

	ep := j.Endpoint(middleware.ClientIdentityFrom(ctx))
	if ep == nil {
		return nil, middleware.NewHttpError(http.StatusNotFound,
			fmt.Errorf("job %q has no endpoint", jobName))
	}
	return ep, nil
}

func (self *zfsJob) listFilesystemVersions(ctx context.Context,
	req *pdu.ListFilesystemVersionsReq,
) (*pdu.ListFilesystemVersionsRes, error) {
	ep, err := self.jobEndpoint(ctx)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, self.timeout)
	defer cancel()

	resp, err := ep.ListFilesystemVersions(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("list snapshots %q: %w", req.Filesystem, err)
	}
	return resp, nil
}

func (self *zfsJob) destroySnapshots(ctx context.Context,
	req *pdu.DestroySnapshotsReq,
) (*pdu.DestroySnapshotsRes, error) {
	ep, err := self.jobEndpoint(ctx)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, self.timeout)
	defer cancel()

	resp, err := ep.DestroySnapshots(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("destroy snapshots %q: %w", req.Filesystem, err)
	}
	return resp, nil
}

func (self *zfsJob) receive(ctx context.Context, req *pdu.ReceiveReq,
	r io.ReadCloser,
) error {
	ep, err := self.jobEndpoint(ctx)
	if err != nil {
		return err
	}

	if _, err = ep.Receive(ctx, req, r); err != nil {
		return fmt.Errorf("create snapshot %q on %q: %w",
			req.To.Name, req.Filesystem, err)
	}
	return nil
}

func (self *zfsJob) send(ctx context.Context, req *pdu.SendReq) (*pdu.SendRes,
	io.ReadCloser, error,
) {
	ep, err := self.jobEndpoint(ctx)
	if err != nil {
		return nil, nil, err
	}

	resp, stream, err := ep.Send(ctx, req)
	if err != nil {
		return nil, nil, fmt.Errorf("send %q from %q to %q: %w",
			req.Filesystem, req.From.Name, req.To.Name, err)
	}
	return resp, stream, nil
}

func (self *zfsJob) sendDry(ctx context.Context, req *pdu.SendReq) (*pdu.SendRes,
	error,
) {
	ep, err := self.jobEndpoint(ctx)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, self.timeout)
	defer cancel()

	resp, err := ep.SendDry(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("dry send %q from %q to %q: %w",
			req.Filesystem, req.From.Name, req.To.Name, err)
	}
	return resp, nil
}

func (self *zfsJob) sendCompleted(ctx context.Context, req *pdu.SendCompletedReq,
) (*pdu.SendCompletedRes, error) {
	ep, err := self.jobEndpoint(ctx)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, self.timeout)
	defer cancel()

	resp, err := ep.SendCompleted(ctx, req)
	if err != nil {
		req := req.OriginalReq
		return nil, fmt.Errorf("complete send %q from %q to %q: %w",
			req.Filesystem, req.From.Name, req.To.Name, err)
	}
	return resp, nil
}

func (self *zfsJob) replicationCursor(ctx context.Context,
	req *pdu.ReplicationCursorReq,
) (*pdu.ReplicationCursorRes, error) {
	ep, err := self.jobEndpoint(ctx)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, self.timeout)
	defer cancel()

	resp, err := ep.ReplicationCursor(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("replication cursor %q: %w", req.Filesystem, err)
	}
	return resp, nil
}
