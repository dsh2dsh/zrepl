package daemon

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"

	"github.com/dsh2dsh/zrepl/internal/logger"
)

type server struct {
	*http.Server

	listener net.Listener

	certFile string
	keyFile  string

	cert *tls.Certificate
	mu   sync.RWMutex
}

func (self *server) Clone() *server {
	return &server{
		Server: self.Server,

		certFile: self.certFile,
		keyFile:  self.keyFile,
	}
}

func (self *server) WithUnix(path string, mode uint32) error {
	self.Addr = path
	laddr, err := net.ResolveUnixAddr("unix", path)
	if err != nil {
		return fmt.Errorf("resolve unix address %q: %w", path, err)
	}

	if self.listener, err = net.ListenUnix("unix", laddr); err != nil {
		return fmt.Errorf("listen unix on %q: %w", path, err)
	} else if mode == 0 {
		return nil
	}

	if err := os.Chmod(path, os.FileMode(mode)); err != nil {
		return fmt.Errorf("change socket mode to %O: %w", mode, err)
	}
	return nil
}

//nolint:wrapcheck // not needed
func (self *server) Serve() error {
	self.initTLSConfig()
	switch {
	case self.listener != nil && self.cert != nil:
		return self.ServeTLS(self.listener, "", "")
	case self.listener != nil:
		return self.Server.Serve(self.listener)
	case self.cert != nil:
		return self.ListenAndServeTLS("", "")
	}
	return self.ListenAndServe()
}

func (self *server) initTLSConfig() {
	if self.cert == nil {
		return
	} else if self.TLSConfig == nil {
		self.TLSConfig = new(tls.Config)
	}
	self.TLSConfig.GetCertificate = self.certificate
}

func (self *server) certificate(*tls.ClientHelloInfo) (*tls.Certificate,
	error,
) {
	self.mu.RLock()
	cert := self.cert
	self.mu.RUnlock()
	return cert, nil
}

func (self *server) Reload(log logger.Logger) error {
	return self.LoadCert(log)
}

func (self *server) LoadCert(log logger.Logger) error {
	if self.certFile == "" {
		return nil
	}
	log.WithField("cert", self.certFile).WithField("key", self.keyFile).
		Info("load certificate")

	cert, err := tls.LoadX509KeyPair(self.certFile, self.keyFile)
	if err != nil {
		return fmt.Errorf("failed load cert from %q, %q: %w",
			self.certFile, self.keyFile, err)
	}

	self.mu.Lock()
	self.cert = &cert
	self.mu.Unlock()
	return nil
}
