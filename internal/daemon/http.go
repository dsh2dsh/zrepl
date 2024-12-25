package daemon

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
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
	if err := unlinkStaleUnix(path); err != nil {
		return err
	}

	self.Addr = path
	laddr, err := net.ResolveUnixAddr("unix", path)
	if err != nil {
		return fmt.Errorf("resolve unix address %q: %w", path, err)
	}

	l, err := net.ListenUnix("unix", laddr)
	if err != nil {
		return fmt.Errorf("listen unix on %q: %w", path, err)
	}

	l.SetUnlinkOnClose(true)
	self.listener = l
	if mode == 0 {
		return nil
	}

	if err := os.Chmod(path, os.FileMode(mode)); err != nil {
		return fmt.Errorf("change socket mode to %O: %w", mode, err)
	}
	return nil
}

func unlinkStaleUnix(path string) error {
	sockdir := filepath.Dir(path)
	stat, err := os.Stat(sockdir)
	switch {
	case err != nil && os.IsNotExist(err):
		if err := os.MkdirAll(sockdir, 0o755); err != nil {
			return fmt.Errorf("cannot mkdir %q: %w", sockdir, err)
		}
		return nil
	case err != nil:
		return fmt.Errorf("cannot stat(2) %q: %w", sockdir, err)
	case !stat.IsDir():
		return fmt.Errorf("not a directory: %q", sockdir)
	}

	_, err = os.Stat(path)
	switch {
	case err == nil:
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("cannot remove stale socket %q: %w", path, err)
		}
	case !os.IsNotExist(err):
		return fmt.Errorf("cannot stat(2) %q: %w", path, err)
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

func (self *server) Reload(log *slog.Logger) error {
	return self.LoadCert(log)
}

func (self *server) LoadCert(log *slog.Logger) error {
	if self.certFile == "" {
		return nil
	}
	log.With(
		slog.String("cert", self.certFile),
		slog.String("key", self.keyFile),
	).Info("load certificate")

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
