package filters

import "github.com/dsh2dsh/zrepl/internal/zfs/dataset"

type filterResult struct {
	parent    *dataset.Path
	included  bool
	recursive bool
}

func (self *filterResult) SetParent(p *dataset.Path) { self.parent = p }

func (self *filterResult) Parent() *dataset.Path { return self.parent }

func (self *filterResult) SetIncluded(included bool) {
	self.included = included
}

func (self *filterResult) Included() bool { return self.included }

func (self *filterResult) SetRecursive(recursive bool) {
	self.recursive = recursive
}

func (self *filterResult) Recursive() bool { return self.recursive }
