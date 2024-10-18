package platformtest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/dsh2dsh/zrepl/internal/zfs"
)

var ZpoolExportTimeout time.Duration = 500 * time.Millisecond

type Zpool struct {
	args ZpoolCreateArgs
}

type ZpoolCreateArgs struct {
	PoolName   string
	ImagePath  string
	ImageSize  int64
	Mountpoint string
}

func (a ZpoolCreateArgs) Validate() error {
	if !filepath.IsAbs(a.ImagePath) {
		return fmt.Errorf("ImagePath must be absolute, got %q", a.ImagePath)
	}
	const minImageSize = 1024
	if a.ImageSize < minImageSize {
		return fmt.Errorf("ImageSize must be > %v, got %v", minImageSize, a.ImageSize)
	}
	if a.Mountpoint == "" || a.Mountpoint[0] != '/' {
		return errors.New("Mountpoint must be an absolute path to a directory")
	}
	if a.PoolName == "" {
		return errors.New("PoolName must not be empty")
	}
	return nil
}

func CreateOrReplaceZpool(ctx context.Context, e Execer, args ZpoolCreateArgs) (*Zpool, error) {
	if err := args.Validate(); err != nil {
		return nil, fmt.Errorf("zpool create args validation error: %w", err)
	}

	// export pool if it already exists (idempotence)
	if _, err := zfs.ZFSGetRawAnySource(ctx, args.PoolName, []string{"name"}); err != nil {
		if _, ok := err.(*zfs.DatasetDoesNotExist); ok {
			// we'll create it shortly
		} else {
			return nil, fmt.Errorf("cannot determine whether test pool %q exists: %w", args.PoolName, err)
		}
	} else {
		// exists, export it, OpenFile will destroy it
		if err := e.RunExpectSuccessNoOutput(ctx, "zpool", "export", args.PoolName); err != nil {
			return nil, fmt.Errorf("cannot destroy test pool %q: %w", args.PoolName, err)
		}
	}

	// clear the mountpoint dir
	if err := os.RemoveAll(args.Mountpoint); err != nil {
		return nil, fmt.Errorf("remove mountpoint dir %q: %w", args.Mountpoint, err)
	}
	if err := os.Mkdir(args.Mountpoint, 0o700); err != nil {
		return nil, fmt.Errorf("create mountpoint dir %q: %w", args.Mountpoint, err)
	}

	// idempotently (re)create the pool image
	image, err := os.OpenFile(args.ImagePath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create image file: %w", err)
	}
	defer image.Close()
	if err := image.Truncate(args.ImageSize); err != nil {
		return nil, fmt.Errorf("create image: truncate: %w", err)
	}
	image.Close()

	// create the pool
	err = e.RunExpectSuccessNoOutput(ctx, "zpool", "create", "-f",
		"-O", "mountpoint="+args.Mountpoint,
		args.PoolName, args.ImagePath,
	)
	if err != nil {
		return nil, fmt.Errorf("zpool create: %w", err)
	}

	return &Zpool{args}, nil
}

func (p *Zpool) Name() string { return p.args.PoolName }

func (p *Zpool) Destroy(ctx context.Context, e Execer) error {
	exportDeadline := time.Now().Add(ZpoolExportTimeout)

	for {
		if time.Now().After(exportDeadline) {
			return fmt.Errorf("could not zpool export (got 'pool is busy'): %s", p.args.PoolName)
		}
		err := e.RunExpectSuccessNoOutput(ctx, "zpool", "export", p.args.PoolName)
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), "pool is busy") {
			runtime.Gosched()
			continue
		}
		return fmt.Errorf("export pool %q: %w", p.args.PoolName, err)
	}

	if err := os.Remove(p.args.ImagePath); err != nil {
		return fmt.Errorf("remove pool image: %w", err)
	}

	if err := os.RemoveAll(p.args.Mountpoint); err != nil {
		return fmt.Errorf("remove mountpoint dir %q: %w", p.args.Mountpoint, err)
	}

	return nil
}
