package zfs

import (
	"context"
	"fmt"
	"slices"

	"github.com/dsh2dsh/zrepl/internal/zfs/zfscmd"
)

type DatasetFilter interface {
	Filter2(path *DatasetPath) (*DatasetPath, bool, error)
	UserSpecifiedDatasets() map[string]bool
}

func ZFSListMapping(ctx context.Context, filter DatasetFilter,
) ([]*DatasetPath, error) {
	props := []string{"name"}
	cmd := NewListCmd(ctx, props, []string{"-r", "-t", "filesystem,volume"})
	v, err, _ := sg.Do(cmd.String(), func() (any, error) {
		datasets, err := listDatasets(ctx, props, nil, cmd)
		if err != nil {
			return nil, err
		}
		return datasets, nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // already wrapped
	}
	return deleteUnmatchedDatasets(ctx, filter, v.([]*DatasetPath))
}

func deleteUnmatchedDatasets(ctx context.Context, filter DatasetFilter,
	datasets []*DatasetPath,
) ([]*DatasetPath, error) {
	var filterErr error
	roots := newRecursiveDatasets()
	unmatchedDatasets := filter.UserSpecifiedDatasets()

	datasets = slices.DeleteFunc(datasets, func(path *DatasetPath) bool {
		delete(unmatchedDatasets, path.ToString())
		if filterErr != nil {
			return false
		}

		root, pass, err := filter.Filter2(path)
		if err != nil {
			filterErr = fmt.Errorf("error calling filter: %w", err)
			return false
		}
		roots.Add(root, path, pass)
		return !pass
	})
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
