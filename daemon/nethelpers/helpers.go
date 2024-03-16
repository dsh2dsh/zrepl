package nethelpers

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
)

func PreparePrivateSockpath(sockpath string) error {
	sockdir := filepath.Dir(sockpath)
	sdstat, err := os.Stat(sockdir)
	if err != nil {
		return fmt.Errorf("cannot stat(2) '%s': %w", sockdir, err)
	}
	if !sdstat.IsDir() {
		return fmt.Errorf("not a directory: %s", sockdir)
	}
	p := sdstat.Mode().Perm()
	if p&0o007 != 0 {
		return fmt.Errorf("socket directory must not be world-accessible: %s (permissions are %#o)", sockdir, p)
	}

	// Maybe things have not been cleaned up before
	s, err := os.Stat(sockpath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("cannot stat(2) '%s': %w", sockpath, err)
	}
	if s.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("unexpected file type at path '%s'", sockpath)
	}
	err = os.Remove(sockpath)
	if err != nil {
		return fmt.Errorf("cannot remove presumably stale socket '%s': %w", sockpath, err)
	}
	return nil
}

func ListenUnixPrivate(sockaddr *net.UnixAddr) (*net.UnixListener, error) {
	if err := PreparePrivateSockpath(sockaddr.Name); err != nil {
		return nil, err
	}

	return net.ListenUnix("unix", sockaddr)
}
