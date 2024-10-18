package tls

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/tlsconf"
	"github.com/dsh2dsh/zrepl/internal/transport"
)

type TLSConnecter struct {
	Address   string
	dialer    net.Dialer
	tlsConfig *tls.Config
}

func TLSConnecterFromConfig(in *config.TLSConnect, parseFlags config.ParseFlags) (*TLSConnecter, error) {
	dialer := net.Dialer{
		Timeout: in.DialTimeout,
	}

	if parseFlags&config.ParseFlagsNoCertCheck != 0 {
		return &TLSConnecter{in.Address, dialer, nil}, nil
	}

	ca, err := tlsconf.ParseCAFile(in.Ca)
	if err != nil {
		return nil, fmt.Errorf("cannot parse ca file: %w", err)
	}

	cert, err := tls.LoadX509KeyPair(in.Cert, in.Key)
	if err != nil {
		return nil, fmt.Errorf("cannot parse cert/key pair: %w", err)
	}

	tlsConfig, err := tlsconf.ClientAuthClient(in.ServerCN, ca, cert)
	if err != nil {
		return nil, fmt.Errorf("cannot build tls config: %w", err)
	}

	return &TLSConnecter{in.Address, dialer, tlsConfig}, nil
}

func (c *TLSConnecter) Connect(dialCtx context.Context) (transport.Wire, error) {
	conn, err := c.dialer.DialContext(dialCtx, "tcp", c.Address)
	if err != nil {
		return nil, err
	}
	tcpConn := conn.(*net.TCPConn)
	tlsConn := tls.Client(conn, c.tlsConfig)
	return newWireAdaptor(tlsConn, tcpConn), nil
}
