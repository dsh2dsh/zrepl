package status

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/dsh2dsh/zrepl/client/jsonclient"
	"github.com/dsh2dsh/zrepl/daemon"
)

type Client struct {
	control *jsonclient.Client
}

func NewClient(network, addr string) (*Client, error) {
	control, err := jsonclient.NewUnix(addr)
	if err != nil {
		return nil, fmt.Errorf("new jsonclient: %w", err)
	}
	return &Client{control: control}, nil
}

func (c *Client) Status() (s daemon.Status, err error) {
	err = c.control.Get(context.Background(),
		daemon.ControlJobEndpointStatus, &s)
	if err != nil {
		err = fmt.Errorf("daemon status: %w", err)
	}
	return
}

func (c *Client) StatusRaw() ([]byte, error) {
	var r json.RawMessage
	err := c.control.Get(context.Background(),
		daemon.ControlJobEndpointStatus, &r)
	if err != nil {
		return nil, fmt.Errorf("daemon status: %w", err)
	}
	return r, nil
}

func (c *Client) signal(job, sig string) error {
	err := c.control.Post(context.Background(),
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

func (c *Client) SignalWakeup(job string) error {
	return c.signal(job, "wakeup")
}

func (c *Client) SignalReset(job string) error {
	return c.signal(job, "reset")
}
