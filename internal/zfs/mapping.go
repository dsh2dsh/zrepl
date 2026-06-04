package zfs

import (
	"context"
	"fmt"
	"iter"
	"slices"

	"github.com/dsh2dsh/zrepl/internal/zfs/zfscmd"
)

type DatasetFilter interface {
	Filter2(path *DatasetPath) (*DatasetPath, bool, error)
	TopFilesystems() (int, iter.Seq[string])
	UserSpecifiedDatasets() map[string]bool
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
	roots := newRecursiveDatasets()
	unmatched := filter.UserSpecifiedDatasets()

	datasets := []*DatasetPath{}
	for _, path := range all {
		root, pass, err := filter.Filter2(path)
		if err != nil {
			return nil, fmt.Errorf("error calling filter: %w", err)
		}
		roots.Append(root, path, pass)
		if pass {
			datasets = append(datasets, path)
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
	children map[*DatasetPath][]*DatasetPath
}

func newRecursiveDatasets() recursiveDatasets {
	return recursiveDatasets{children: map[*DatasetPath][]*DatasetPath{}}
}

func (self *recursiveDatasets) Append(root, path *DatasetPath, included bool) {
	if root == nil {
		return
	}

	children := self.children[root]
	if !included {
		children[0].WithExcluded(path)
		return
	}

	if n := len(children); n == 0 && root.Recursive() {
		path.SetRecursive()
	} else if n > 0 {
		path.SetRecursiveParent(children[0])
	}
	children = append(children, path)
	self.children[root] = children
}
