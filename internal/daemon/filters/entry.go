package filters

import (
	"cmp"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/zfs"
)

func NewItem(in config.DatasetFilter) (*filterItem, error) {
	item := &filterItem{
		pattern:      in.Pattern,
		mapping:      !in.Exclude,
		recursive:    in.Recursive,
		shellPattern: in.Shell,
	}
	if err := item.init(); err != nil {
		return nil, err
	}
	return item, nil
}

type filterItem struct {
	pattern   string
	mapping   bool
	recursive bool

	shellPattern bool
	shellPath    *zfs.DatasetPath

	path *zfs.DatasetPath
}

func (self *filterItem) init() error {
	if self.shellPattern {
		return self.initShellPattern()
	} else if self.path != nil {
		return nil
	}

	path, err := zfs.NewDatasetPath(self.pattern)
	if err != nil {
		return fmt.Errorf("pattern %q is not a dataset path: %w",
			self.pattern, err)
	}

	if self.recursive {
		path.SetRecursive()
	}
	self.path = path
	return nil
}

func (self *filterItem) initShellPattern() error {
	if _, err := filepath.Match(self.pattern, ""); err != nil {
		return fmt.Errorf("invalid shell pattern %q: %w", self.pattern, err)
	}

	base := shellBaseDir(self.pattern)
	if base == "" {
		return nil
	}

	path, err := zfs.NewDatasetPath(base)
	if err != nil {
		return fmt.Errorf(
			"failed extract dataset path %q from shell pattern %q: %w",
			base, self.pattern, err)
	}
	self.shellPath = path
	return nil
}

func shellBaseDir(pattern string) string {
	n := strings.IndexAny(pattern, `*?[\`)
	if n < 0 {
		return pattern
	}

	n = strings.LastIndex(pattern[:n], "/")
	if n < 0 {
		return ""
	}
	return pattern[:n]
}

func (self *filterItem) WithMapping(v bool) *filterItem {
	self.mapping = v
	return self
}

func (self *filterItem) SetShellPattern(pattern string) error {
	self.pattern = pattern
	self.shellPattern = true
	return self.init()
}

func (self *filterItem) DatasetPath() *zfs.DatasetPath { return self.path }

func (self *filterItem) Mapping() bool { return self.mapping }

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
	if self.mapping && self.recursive {
		if self.path != nil && !self.path.Empty() {
			return self.path
		}
	}
	return nil
}

func (self *filterItem) ParentFilesystem() *zfs.DatasetPath {
	if !self.mapping {
		return nil
	}

	path := self.path
	if self.shellPattern {
		path = self.shellPath
	}

	if path.Empty() {
		return nil
	}
	return path
}

func (self *filterItem) HasPrefix(prefix *zfs.DatasetPath) bool {
	switch {
	case self.shellPath != nil:
		return self.shellPath.HasPrefix(prefix)
	case self.path != nil:
		return self.path.HasPrefix(prefix)
	}
	return strings.HasPrefix(self.pattern, prefix.ToString()+"/")
}
