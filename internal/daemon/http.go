package daemon

import (
	"fmt"
	"net"
	"net/http"
	"os"
)

type server struct {
	*http.Server

	listener  net.Listener
	cert, key string
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

func (self *server) Serve() error {
	switch {
	case self.listener != nil && self.cert != "":
		return self.ServeTLS(self.listener, self.cert, self.key)
	case self.listener != nil:
		return self.Server.Serve(self.listener)
	case self.cert != "":
		return self.ListenAndServeTLS(self.cert, self.key)
	}
	return self.ListenAndServe()
}
