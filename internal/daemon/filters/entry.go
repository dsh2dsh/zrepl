package filters

import (
	"cmp"
	"fmt"
	"path/filepath"

	"github.com/dsh2dsh/zrepl/internal/zfs"
)

type filterItem struct {
	pattern   string
	mapping   bool
	recursive bool
	path      *zfs.DatasetPath

	shellPattern bool
}

func (self *filterItem) Init() error {
	if self.shellPattern {
		if _, err := filepath.Match(self.pattern, ""); err != nil {
			return fmt.Errorf("invalid shell pattern %q: %w", self.pattern, err)
		}
	} else if self.path == nil {
		path, err := zfs.NewDatasetPath(self.pattern)
		if err != nil {
			return fmt.Errorf("pattern %q is not a dataset path: %w",
				self.pattern, err)
		}
		if self.recursive {
			path.SetRecursive()
		}
		self.path = path
	}
	return nil
}

func (self *filterItem) Clone() *filterItem {
	cloned := *self
	return &cloned
}

func (self *filterItem) Match(p *zfs.DatasetPath) (bool, error) {
	switch {
	case self.shellPattern:
		matched, err := filepath.Match(self.pattern, p.ToString())
		if err != nil {
			return matched, fmt.Errorf("matching %q to %q: %w",
				p.ToString(), self.pattern, err)
		}
		return matched, nil
	case self.recursive:
		return p.HasPrefix(self.path), nil
	}
	return self.path.Equal(p), nil
}

func (self *filterItem) CompatCompare(b *filterItem) int {
	switch {
	case self.path == nil && b.path == nil:
		return 0
	case self.path != nil && b.path == nil:
		return -1
	case self.path == nil && b.path != nil:
		return 1
	case self.recursive && !b.recursive:
		return -1
	case !self.recursive && b.recursive:
		return 1
	}
	return cmp.Compare(self.path.Length(), b.path.Length())
}

func (self *filterItem) RecursiveDataset() *zfs.DatasetPath {
	if !self.recursive || !self.mapping {
		return nil
	}
	return self.path
}
