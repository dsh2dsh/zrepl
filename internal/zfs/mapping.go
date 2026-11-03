package zfs

import (
	"context"
	"fmt"
	"iter"
	"slices"

	"github.com/dsh2dsh/zrepl/internal/zfs/zfscmd"
)

type DatasetFilter interface {
	Filtered(path *DatasetPath) (Filtered, error)
	TopFilesystems() (int, iter.Seq[string])
	UserSpecifiedDatasets() map[string]bool
}

type Filtered interface {
	SetParent(*DatasetPath)
	Parent() *DatasetPath

	SetIncluded(bool)
	Included() bool

	SetRecursive(bool)
	Recursive() bool
}

func ZFSListMapping(ctx context.Context, filter DatasetFilter,
) ([]*DatasetPath, error) {
	props := []string{"name", "written"}
	args := []string{"-r", "-t", "filesystem,volume"}
	n, pfs := filter.TopFilesystems()
	if n != 0 {
		args = slices.AppendSeq(slices.Grow(args, n), pfs)
	}
	cmd := NewListCmd(ctx, props, args)

	v, err, _ := sg.Do(cmd.String(), func() (any, error) {
		datasets := []*DatasetPath{}
		for fields, err := range ListIter(ctx, props, nil, cmd) {
			if err != nil {
				return nil, err
			}
			path, err := NewDatasetPath(fields[0], WithWritten(fields[1]))
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
	return filterDatasets(ctx, filter, v.([]*DatasetPath))
}

func filterDatasets(ctx context.Context, filter DatasetFilter,
	all []*DatasetPath,
) ([]*DatasetPath, error) {
	parents := newRecursiveDatasets()
	unmatched := filter.UserSpecifiedDatasets()

	datasets := []*DatasetPath{}
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
	parents map[*DatasetPath]*DatasetPath
}

func newRecursiveDatasets() recursiveDatasets {
	return recursiveDatasets{parents: make(map[*DatasetPath]*DatasetPath)}
}

func (self *recursiveDatasets) Add(root, path *DatasetPath, included bool) {
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
