package status

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/dsh2dsh/zrepl/client/jsonclient"
	"github.com/dsh2dsh/zrepl/daemon"
)

func NewClient(network, addr string) (*Client, error) {
	control, err := jsonclient.NewUnix(addr)
	if err != nil {
		return nil, fmt.Errorf("new jsonclient: %w", err)
	}
	return &Client{control: control}, nil
}

type Client struct {
	control *jsonclient.Client
}

func (self *Client) Status() (s daemon.Status, err error) {
	err = self.control.Get(context.Background(),
		daemon.ControlJobEndpointStatus, &s)
	if err != nil {
		err = fmt.Errorf("daemon status: %w", err)
	}
	return
}

func (self *Client) StatusRaw() ([]byte, error) {
	var r json.RawMessage
	err := self.control.Get(context.Background(),
		daemon.ControlJobEndpointStatus, &r)
	if err != nil {
		return nil, fmt.Errorf("daemon status: %w", err)
	}
	return r, nil
}

func (self *Client) SignalWakeup(job string) error {
	return self.signal(job, "wakeup")
}

func (self *Client) SignalReset(job string) error {
	return self.signal(job, "reset")
}

func (self *Client) signal(job, sig string) error {
	err := self.control.Post(context.Background(),
		daemon.ControlJobEndpointSignal,
		struct {
			Name string
			Op   string
		}{
			Name: job,
			Op:   sig,
		}, nil,
	)
	if err != nil {
		return fmt.Errorf("daemon signal %q to job %q: %w", sig, job, err)
	}
	return nil
}
