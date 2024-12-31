package endpoint

import (
	"context"
	"errors"
	"iter"
	"log/slog"
	"runtime"

	"golang.org/x/sync/errgroup"

	"github.com/dsh2dsh/zrepl/internal/replication/logic/pdu"
	"github.com/dsh2dsh/zrepl/internal/zfs"
)

func destroySnapshots(ctx context.Context, concurrency, fsCount int,
	reqs iter.Seq2[*pdu.DestroySnapshots, error],
) (*pdu.DestroySnapshotsRes, error) {
	var g errgroup.Group
	if concurrency < 1 {
		concurrency = runtime.GOMAXPROCS(0)
	}
	g.SetLimit(concurrency)

	getLogger(ctx).With(
		slog.Int("fs_count", fsCount),
		slog.Int("concurrency", concurrency),
	).Info("destroy snapshots")

	fsRes := make([]pdu.DestroyedSnapshots, 0, fsCount)
	for r, err := range reqs {
		fs, snaps := r.Filesystem, r.Snapshots
		fsRes = append(fsRes, pdu.DestroyedSnapshots{Filesystem: fs})
		destroyed := &fsRes[len(fsRes)-1]
		if ctx.Err() != nil {
			destroyed.Error = context.Cause(ctx).Error()
			continue
		} else if err != nil {
			destroyed.Error = err.Error()
			continue
		}
		g.Go(func() error {
			failed, err := destroyOneSnapshots(ctx, fs, snaps)
			if err != nil {
				destroyed.Error = err.Error()
			} else if len(failed) != 0 {
				destroyed.Results = failed
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err //nolint:wrapcheck // not external error
	}
	return &pdu.DestroySnapshotsRes{Filesystems: fsRes}, nil
}

func destroyOneSnapshots(ctx context.Context, lp string, snapNames []string,
) (destroyed []pdu.DestroySnapshotRes, _ error) {
	destroy := make([]zfs.DestroySnapOp, len(snapNames))
	for i, name := range snapNames {
		destroy[i].Name = name
	}

	getLogger(ctx).With(
		slog.String("fs", lp),
		slog.Int("count", len(destroy)),
	).Debug("destroying snapshots")

	zfs.ZFSDestroyFilesystemVersions(ctx, lp, destroy)
	for i := range destroy {
		if err := destroy[i].Err; err != nil {
			destroyed = append(destroyed, pdu.DestroySnapshotRes{
				Name: destroy[i].Name,
			})
			failed := &destroyed[len(destroyed)-1]
			var de *zfs.DestroySnapshotsError
			if errors.As(err, &de) && len(de.Reason) == 1 {
				failed.Error = de.Reason[0]
			} else {
				failed.Error = err.Error()
			}
		}
	}
	return destroyed, nil
}
