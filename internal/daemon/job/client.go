package job

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/dsh2dsh/zrepl/internal/client/jsonclient"
	"github.com/dsh2dsh/zrepl/internal/replication/logic/pdu"
)

const (
	EpListFilesystems = iota
	EpListFilesystemVersions
	EpDestroySnapshots
	EpWaitForConnectivity

	EpReceive

	EpSend
	EpSendDry
	EpSendCompleted
	EpReplicationCursor

	numEndpoints
)

var allEndpoints = [numEndpoints]string{
	"/zfs/datasets/",  // epListFilesystems
	"/zfs/snapshots/", // epDestroySnapshots
	"/zfs/destroy/",   // epListFilesystemVersions
	"/zfs/health/",    // epWaitForConnectivity

	"/zfs/recv/", // epReceive

	"/zfs/send/",    // epSend
	"/zfs/drysend/", // epSendDry
	"/zfs/sendok/",  // epSendCompleted
	"/zfs/cursor/",  // epReplicationCursor
}

func NewClient(jobName string, client *jsonclient.Client) *Client {
	c := &Client{
		jsonClient: client,
		endpoints:  EndpointNames(jobName),
		timeout:    time.Minute,
	}
	return c
}

func EndpointNames(jobName string) []string {
	endpoints := make([]string, numEndpoints)
	for i, prefix := range allEndpoints {
		endpoints[i] = prefix + jobName
	}
	return endpoints
}

type Client struct {
	jsonClient *jsonclient.Client
	endpoints  []string

	timeout time.Duration
}

var _ Endpoint = (*Client)(nil)

func (self *Client) WithTimeout(d time.Duration) *Client {
	if d > 0 {
		self.timeout = d
	}
	return self
}

func (self *Client) endpoint(i int) string { return self.endpoints[i] }

func (self *Client) json() *jsonclient.Client { return self.jsonClient }

func (self *Client) ListFilesystems(ctx context.Context,
	_ *pdu.ListFilesystemReq,
) (*pdu.ListFilesystemRes, error) {
	ctx, cancel := context.WithTimeout(ctx, self.timeout)
	defer cancel()

	ep := self.endpoint(EpListFilesystems)
	resp := new(pdu.ListFilesystemRes)
	if err := self.json().Get(ctx, ep, resp); err != nil {
		return nil, fmt.Errorf("endpoint %q: %w", ep, err)
	}
	return resp, nil
}

func (self *Client) ListFilesystemVersions(ctx context.Context,
	req *pdu.ListFilesystemVersionsReq,
) (*pdu.ListFilesystemVersionsRes, error) {
	ctx, cancel := context.WithTimeout(ctx, self.timeout)
	defer cancel()

	ep := self.endpoint(EpListFilesystemVersions)
	resp := new(pdu.ListFilesystemVersionsRes)
	if err := self.json().Post(ctx, ep, req, resp); err != nil {
		return nil, fmt.Errorf("endpoint %q: %w", ep, err)
	}
	return resp, nil
}

func (self *Client) DestroySnapshots(ctx context.Context,
	req *pdu.DestroySnapshotsReq,
) (*pdu.DestroySnapshotsRes, error) {
	ctx, cancel := context.WithTimeout(ctx, self.timeout)
	defer cancel()

	ep := self.endpoint(EpDestroySnapshots)
	resp := new(pdu.DestroySnapshotsRes)
	if err := self.json().Post(ctx, ep, req, resp); err != nil {
		return nil, fmt.Errorf("endpoint %q: %w", ep, err)
	}
	return resp, nil
}

func (self *Client) WaitForConnectivity(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, self.timeout)
	defer cancel()

	ep := self.endpoint(EpWaitForConnectivity)
	if err := self.json().Get(ctx, ep, nil); err != nil {
		return fmt.Errorf("endpoint %q: %w", ep, err)
	}
	return nil
}

func (self *Client) Receive(ctx context.Context, req *pdu.ReceiveReq,
	receive io.ReadCloser,
) (*pdu.ReceiveRes, error) {
	defer receive.Close()
	ep := self.endpoint(EpReceive)
	if err := self.json().PostStream(ctx, ep, req, nil, receive); err != nil {
		return nil, fmt.Errorf("endpoint %q: %w", ep, err)
	}
	return new(pdu.ReceiveRes), nil
}

func (self *Client) Send(ctx context.Context, req *pdu.SendReq,
) (*pdu.SendRes, io.ReadCloser, error) {
	ep := self.endpoint(EpSend)
	resp := new(pdu.SendRes)
	r, err := self.json().PostResponseStream(ctx, ep, req, resp)
	if err != nil {
		return nil, nil, fmt.Errorf("endpoint %q: %w", ep, err)
	}
	return resp, r, nil
}

func (self *Client) SendDry(ctx context.Context, req *pdu.SendReq,
) (*pdu.SendRes, error) {
	ctx, cancel := context.WithTimeout(ctx, self.timeout)
	defer cancel()

	ep := self.endpoint(EpSendDry)
	resp := new(pdu.SendRes)
	if err := self.json().Post(ctx, ep, req, resp); err != nil {
		return nil, fmt.Errorf("endpoint %q: %w", ep, err)
	}
	return resp, nil
}

func (self *Client) SendCompleted(ctx context.Context,
	req *pdu.SendCompletedReq,
) (*pdu.SendCompletedRes, error) {
	ctx, cancel := context.WithTimeout(ctx, self.timeout)
	defer cancel()

	ep := self.endpoint(EpSendCompleted)
	if err := self.json().Post(ctx, ep, req, nil); err != nil {
		return nil, fmt.Errorf("endpoint %q: %w", ep, err)
	}
	return new(pdu.SendCompletedRes), nil
}

func (self *Client) ReplicationCursor(ctx context.Context,
	req *pdu.ReplicationCursorReq,
) (*pdu.ReplicationCursorRes, error) {
	ctx, cancel := context.WithTimeout(ctx, self.timeout)
	defer cancel()

	resp := struct {
		Result struct {
			Guid     uint64
			Notexist bool
		}
	}{}

	ep := self.endpoint(EpReplicationCursor)
	if err := self.json().Post(ctx, ep, req, &resp); err != nil {
		return nil, fmt.Errorf("endpoint %q: %w", ep, err)
	}

	if resp.Result.Notexist {
		return &pdu.ReplicationCursorRes{
			Result: &pdu.ReplicationCursorRes_Result{
				Notexist: resp.Result.Notexist,
			},
		}, nil
	}

	return &pdu.ReplicationCursorRes{
		Result: &pdu.ReplicationCursorRes_Result{Guid: resp.Result.Guid},
	}, nil
}
