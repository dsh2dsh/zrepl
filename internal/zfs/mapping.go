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
	var filterErr error
	roots := newRecursiveDatasets()
	unmatchedDatasets := filter.UserSpecifiedDatasets()

	datasets := []*DatasetPath{}
	for _, path := range all {
		root, pass, err := filter.Filter2(path)
		if err != nil {
			return nil, fmt.Errorf("error calling filter: %w", err)
		}
		roots.Add(root, path, pass)
		if pass {
			datasets = append(datasets, path)
		}
		delete(unmatchedDatasets, path.ToString())
	}
	roots.UpdateChildren()

	prom.ZFSListUnmatchedUserSpecifiedDatasetCount.
		WithLabelValues(zfscmd.GetJobID(ctx)).
		Add(float64(len(unmatchedDatasets)))
	return datasets, filterErr
}

// --------------------------------------------------

func newRecursiveDatasets() recursiveDatasets {
	return recursiveDatasets{
		children: make(map[*DatasetPath][]*DatasetPath),
		skip:     make(map[*DatasetPath]struct{}),
	}
}

type recursiveDatasets struct {
	children map[*DatasetPath][]*DatasetPath
	skip     map[*DatasetPath]struct{}
}

func (self *recursiveDatasets) Add(root *DatasetPath, path *DatasetPath,
	included bool,
) {
	switch {
	case self.skipped(root):
	case included:
		self.children[root] = append(self.children[root], path)
	default:
		delete(self.children, root)
		self.skip[root] = struct{}{}
	}
}

func (self *recursiveDatasets) skipped(root *DatasetPath) bool {
	if root == nil {
		return true
	}
	_, ok := self.skip[root]
	return ok
}

func (self *recursiveDatasets) UpdateChildren() {
	for root, children := range self.children {
		for _, p := range children {
			p.SetRecursiveParent(root)
		}
	}
}
