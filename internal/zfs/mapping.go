package zfs

import (
	"context"
	"fmt"
	"slices"

	"github.com/dsh2dsh/zrepl/internal/zfs/dataset"
	"github.com/dsh2dsh/zrepl/internal/zfs/filter"
	"github.com/dsh2dsh/zrepl/internal/zfs/zfscmd"
)

func ZFSListMapping(ctx context.Context, filter *filter.Filter,
) ([]*dataset.Path, error) {
	props := []string{"name", "written"}
	args := []string{"-r", "-t", "filesystem,volume"}
	n, pfs := filter.TopFilesystems()
	if n != 0 {
		args = slices.AppendSeq(slices.Grow(args, n), pfs)
	}
	cmd := NewListCmd(ctx, props, args)

	v, err, _ := sg.Do(cmd.String(), func() (any, error) {
		datasets := []*dataset.Path{}
		for fields, err := range ListIter(ctx, props, nil, cmd) {
			if err != nil {
				return nil, err
			}
			path, err := dataset.NewPath(fields[0],
				dataset.WithWritten(fields[1]))
			if err != nil {
				return nil, err
			}
			datasets = append(datasets, path)
		}
		return datasets, nil
	})

	if err != nil {
		return nil, err //nolint:wrapcheck // already wrapped
	}
	return filterDatasets(ctx, filter, v.([]*dataset.Path))
}

func filterDatasets(ctx context.Context, filter *filter.Filter,
	all []*dataset.Path,
) ([]*dataset.Path, error) {
	parents := newRecursiveDatasets()
	unmatched := filter.UserSpecifiedDatasets()

	datasets := []*dataset.Path{}
	for _, path := range all {
		filtered, err := filter.Filtered(path)
		if err != nil {
			return nil, fmt.Errorf("error calling filter: %w", err)
		}

		parents.Add(filtered.Parent(), path, filtered.Included())
		if filtered.Included() {
			datasets = append(datasets, path)
		} else {
			path.WithRecursive(filtered.Recursive())
		}
		delete(unmatched, path.ToString())
	}

	prom.ZFSListUnmatchedUserSpecifiedDatasetCount.
		WithLabelValues(zfscmd.GetJobID(ctx)).
		Add(float64(len(unmatched)))
	return datasets, nil
}

// --------------------------------------------------

type recursiveDatasets struct {
	parents map[*dataset.Path]*dataset.Path
}

func newRecursiveDatasets() recursiveDatasets {
	return recursiveDatasets{parents: make(map[*dataset.Path]*dataset.Path)}
}

func (self *recursiveDatasets) Add(root, path *dataset.Path, included bool) {
	if root == nil || !root.Recursive() {
		return
	}

	if path.Equal(root) {
		path.WithRecursive(true)
		self.parents[root] = path
		if !included {
			path.WithExcluded(path)
		}
		return
	}

	parent := self.parents[root]
	if included {
		path.SetRecursiveParent(parent)
	} else {
		parent.WithExcluded(path)
	}
}
