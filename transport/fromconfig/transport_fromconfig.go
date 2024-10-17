// Package fromconfig instantiates transports based on zrepl config structures
// (see package config).
package fromconfig

import (
	"fmt"

	"github.com/dsh2dsh/zrepl/config"
	"github.com/dsh2dsh/zrepl/transport"
	"github.com/dsh2dsh/zrepl/transport/ssh"
	"github.com/dsh2dsh/zrepl/transport/tcp"
	"github.com/dsh2dsh/zrepl/transport/tls"
)

func ListenerFactoryFromConfig(g *config.Global, in config.ServeEnum,
	parseFlags config.ParseFlags,
) (transport.AuthenticatedListenerFactory, error) {
	switch v := in.Ret.(type) {
	case *config.TCPServe:
		return tcp.TCPListenerFactoryFromConfig(g, v)
	case *config.TLSServe:
		return tls.TLSListenerFactoryFromConfig(g, v, parseFlags)
	case *config.StdinserverServer:
		return ssh.MultiStdinserverListenerFactoryFromConfig(g, v)
	case *config.LocalServe:
		// l, err = local.LocalListenerFactoryFromConfig(g, v)
	default:
		return nil, fmt.Errorf("internal error: unknown serve type %T", v)
	}
	return nil, nil
}

func ConnecterFromConfig(g *config.Global, in config.ConnectEnum,
	parseFlags config.ParseFlags,
) (transport.Connecter, error) {
	switch v := in.Ret.(type) {
	case *config.SSHStdinserverConnect:
		return ssh.SSHStdinserverConnecterFromConfig(v)
	case *config.TCPConnect:
		return tcp.TCPConnecterFromConfig(v)
	case *config.TLSConnect:
		return tls.TLSConnecterFromConfig(v, parseFlags)
	case *config.LocalConnect:
		// connecter, err = local.LocalConnecterFromConfig(v)
	default:
		panic(fmt.Sprintf("implementation error: unknown connecter type %T", v))
	}
	return nil, nil
}
