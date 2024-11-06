package rpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"

	"github.com/dsh2dsh/zrepl/internal/replication/logic"
	"github.com/dsh2dsh/zrepl/internal/replication/logic/pdu"
	"github.com/dsh2dsh/zrepl/internal/rpc/dataconn"
	"github.com/dsh2dsh/zrepl/internal/rpc/grpcclientidentity/grpchelper"
	"github.com/dsh2dsh/zrepl/internal/rpc/versionhandshake"
	"github.com/dsh2dsh/zrepl/internal/transport"
	"github.com/dsh2dsh/zrepl/internal/util/envconst"
)

// Client implements the active side of a replication setup.
// It satisfies the Endpoint, Sender and Receiver interface defined by package replication.
type Client struct {
	dataClient    *dataconn.Client
	controlClient pdu.ReplicationClient // this the grpc client instance, see constructor
	controlConn   *grpc.ClientConn
	loggers       Loggers
	closed        chan struct{}
}

var (
	_ logic.Endpoint = &Client{}
	_ logic.Sender   = &Client{}
	_ logic.Receiver = &Client{}
)

type DialContextFunc = func(ctx context.Context, network string, addr string) (net.Conn, error)

// config must be validated, NewClient will panic if it is not valid
func NewClient(cn transport.Connecter, loggers Loggers) *Client {
	cn = versionhandshake.Connecter(cn, envconst.Duration("ZREPL_RPC_CLIENT_VERSIONHANDSHAKE_TIMEOUT", 10*time.Second))

	muxedConnecter := mux(cn)

	c := &Client{
		loggers: loggers,
		closed:  make(chan struct{}),
	}
	grpcConn := grpchelper.ClientConn(muxedConnecter.control, loggers.Control)

	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			<-c.closed
			cancel()
		}()
		defer cancel()
		for ctx.Err() == nil {
			state := grpcConn.GetState()
			loggers.General.WithField("grpc_state", state.String()).Debug("grpc state change")
			grpcConn.WaitForStateChange(ctx, state)
		}
	}()
	c.controlClient = pdu.NewReplicationClient(grpcConn)
	c.controlConn = grpcConn

	c.dataClient = dataconn.NewClient(muxedConnecter.data, loggers.Data)
	return c
}

func (c *Client) Close() {
	close(c.closed)
	if err := c.controlConn.Close(); err != nil {
		c.loggers.General.WithError(err).Error("cannot close control connection")
	}
	// TODO c.dataClient should have Close()
}

// callers must ensure that the returned io.ReadCloser is closed
// TODO expose dataClient interface to the outside world
func (c *Client) Send(ctx context.Context, r *pdu.SendReq) (*pdu.SendRes, io.ReadCloser, error) {
	// TODO the returned sendStream may return a read error created by the remote side
	res, stream, err := c.dataClient.ReqSend(ctx, r)
	if err != nil {
		return nil, nil, err
	}
	if stream == nil {
		return res, nil, nil
	}

	return res, stream, nil
}

func (c *Client) Receive(ctx context.Context, req *pdu.ReceiveReq, stream io.ReadCloser) (*pdu.ReceiveRes, error) {
	return c.dataClient.ReqRecv(ctx, req, stream)
}

func (c *Client) SendDry(ctx context.Context, in *pdu.SendReq) (*pdu.SendRes, error) {
	return c.controlClient.SendDry(ctx, in)
}

func (c *Client) ListFilesystems(ctx context.Context, in *pdu.ListFilesystemReq) (*pdu.ListFilesystemRes, error) {
	return c.controlClient.ListFilesystems(ctx, in)
}

func (c *Client) ListFilesystemVersions(ctx context.Context, in *pdu.ListFilesystemVersionsReq) (*pdu.ListFilesystemVersionsRes, error) {
	return c.controlClient.ListFilesystemVersions(ctx, in)
}

func (c *Client) DestroySnapshots(ctx context.Context, in *pdu.DestroySnapshotsReq) (*pdu.DestroySnapshotsRes, error) {
	return c.controlClient.DestroySnapshots(ctx, in)
}

func (c *Client) ReplicationCursor(ctx context.Context, in *pdu.ReplicationCursorReq) (*pdu.ReplicationCursorRes, error) {
	return c.controlClient.ReplicationCursor(ctx, in)
}

func (c *Client) SendCompleted(ctx context.Context, in *pdu.SendCompletedReq) (*pdu.SendCompletedRes, error) {
	return c.controlClient.SendCompleted(ctx, in)
}

func (c *Client) WaitForConnectivity(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	msg := uuid.New().String()
	req := pdu.PingReq{Message: msg}
	var ctrlOk, dataOk int32
	loggers := GetLoggersOrPanic(ctx)
	var wg sync.WaitGroup
	wg.Add(2)
	checkRes := func(res *pdu.PingRes, err error, logger Logger, okVar *int32) {
		if err == nil && res.GetEcho() != req.GetMessage() {
			err = errors.New("pilot message not echoed correctly")
		}
		if err == context.Canceled {
			err = nil
		}
		if err != nil {
			logger.WithError(err).Error("ping failed")
			atomic.StoreInt32(okVar, 0)
			cancel()
		} else {
			atomic.StoreInt32(okVar, 1)
		}
	}
	go func() {
		defer wg.Done()
		ctrl, ctrlErr := c.controlClient.Ping(ctx, &req, grpc.WaitForReady(true))
		checkRes(ctrl, ctrlErr, loggers.Control, &ctrlOk)
	}()
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			data, dataErr := c.dataClient.ReqPing(ctx, &req)
			// dataClient uses transport.Connecter, which doesn't expose WaitForReady(true)
			// => we need to mask dial timeouts
			if err, ok := dataErr.(interface{ Timeout() bool }); ok && err.Timeout() {
				// Rate-limit pings here in case Timeout() == true is a mis-classification
				// or returns immediately (this is a tight loop in that case)
				// TODO keep this in lockstep with controlClient
				// 		=> don't use FailFast for control, but check that both control and data worked
				time.Sleep(envconst.Duration("ZREPL_RPC_DATACONN_PING_SLEEP", 1*time.Second))
				continue
			}
			// it's not a dial timeout,
			checkRes(data, dataErr, loggers.Data, &dataOk)
			return
		}
	}()
	wg.Wait()
	var what string
	if ctrlOk == 1 && dataOk == 1 {
		return nil
	}
	if ctrlOk == 0 {
		what += "control"
	}
	if dataOk == 0 {
		if len(what) > 0 {
			what += " and data"
		} else {
			what += "data"
		}
	}
	return fmt.Errorf("%s rpc failed to respond to ping rpcs", what)
}

func (c *Client) ResetConnectBackoff() {
	c.controlConn.ResetConnectBackoff()
}
