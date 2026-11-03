package filters

import "github.com/dsh2dsh/zrepl/internal/zfs"

type filterResult struct {
	parent    *zfs.DatasetPath
	included  bool
	recursive bool
}

func (self *filterResult) SetParent(p *zfs.DatasetPath) { self.parent = p }

func (self *filterResult) Parent() *zfs.DatasetPath { return self.parent }

func (self *filterResult) SetIncluded(included bool) {
	self.included = included
}

func (self *filterResult) Included() bool { return self.included }

func (self *filterResult) SetRecursive(recursive bool) {
	self.recursive = recursive
}

func (self *filterResult) Recursive() bool { return self.recursive }
