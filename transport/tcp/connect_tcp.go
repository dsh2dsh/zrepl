package tcp

import (
	"context"
	"net"

	"github.com/dsh2dsh/zrepl/config"
	"github.com/dsh2dsh/zrepl/transport"
)

type TCPConnecter struct {
	Address string
	dialer  net.Dialer
}

func TCPConnecterFromConfig(in *config.TCPConnect) (*TCPConnecter, error) {
	dialer := net.Dialer{
		Timeout: in.DialTimeout,
	}

	return &TCPConnecter{in.Address, dialer}, nil
}

func (c *TCPConnecter) Connect(dialCtx context.Context) (transport.Wire, error) {
	conn, err := c.dialer.DialContext(dialCtx, "tcp", c.Address)
	if err != nil {
		return nil, err
	}
	return conn.(*net.TCPConn), nil
}
