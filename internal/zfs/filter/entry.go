package filter

import (
	"cmp"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/zfs/dataset"
)

func NewItem(in config.DatasetFilter) (*item, error) {
	item := &item{
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

type item struct {
	pattern   string
	mapping   bool
	recursive *bool

	shellPattern bool
	shellPath    *dataset.Path

	path *dataset.Path
}

func (self *item) init() error {
	if self.shellPattern {
		return self.initShellPattern()
	} else if self.path != nil {
		return nil
	}

	path, err := dataset.NewPath(self.pattern)
	if err != nil {
		return fmt.Errorf("pattern %q is not a dataset path: %w",
			self.pattern, err)
	}

	self.path = path.WithRecursive(self.Recursive())
	return nil
}

func (self *item) initShellPattern() error {
	if _, err := filepath.Match(self.pattern, ""); err != nil {
		return fmt.Errorf("invalid shell pattern %q: %w", self.pattern, err)
	}

	base := shellBaseDir(self.pattern)
	if base == "" {
		return nil
	}

	path, err := dataset.NewPath(base)
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

func (self *item) WithMapping(v bool) *item {
	self.mapping = v
	return self
}

func (self *item) SetShellPattern(pattern string) error {
	self.pattern = pattern
	self.shellPattern = true
	return self.init()
}

func (self *item) DatasetPath() *dataset.Path { return self.path }

func (self *item) Mapping() bool { return self.mapping }

func (self *item) Recursive() bool {
	if self.recursive != nil {
		return *self.recursive
	}
	return !self.mapping
}

func (self *item) Clone() *item {
	cloned := *self
	return &cloned
}

func (self *item) Match(p *dataset.Path) (bool, error) {
	switch {
	case self.shellPattern:
		matched, err := filepath.Match(self.pattern, p.ToString())
		if err != nil {
			return matched, fmt.Errorf("matching %q to %q: %w",
				p.ToString(), self.pattern, err)
		}
		return matched, nil
	case self.Recursive():
		return p.HasPrefix(self.path), nil
	}
	return self.path.Equal(p), nil
}

func (self *item) CompatCompare(b *item) int {
	switch {
	case self.path == nil && b.path == nil:
		return 0
	case self.path != nil && b.path == nil:
		return -1
	case self.path == nil && b.path != nil:
		return 1
	case self.Recursive() && !b.Recursive():
		return -1
	case !self.Recursive() && b.Recursive():
		return 1
	}
	return cmp.Compare(self.path.Length(), b.path.Length())
}

func (self *item) RecursiveDataset() *dataset.Path {
	if self.mapping && self.Recursive() {
		if self.path != nil && !self.path.Empty() {
			return self.path
		}
	}
	return nil
}

func (self *item) ParentFilesystem() *dataset.Path {
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

func (self *item) HasPrefix(prefix *dataset.Path) bool {
	switch {
	case self.shellPath != nil:
		return self.shellPath.HasPrefix(prefix)
	case self.path != nil:
		return self.path.HasPrefix(prefix)
	}
	return strings.HasPrefix(self.pattern, prefix.ToString()+"/")
}
