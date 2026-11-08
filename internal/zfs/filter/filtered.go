package filter

import "github.com/dsh2dsh/zrepl/internal/zfs/dataset"

type Filtered struct {
	parent    *dataset.Path
	included  bool
	recursive bool
}

func (self *Filtered) SetParent(p *dataset.Path) { self.parent = p }

func (self *Filtered) Parent() *dataset.Path { return self.parent }

func (self *Filtered) SetIncluded(included bool) {
	self.included = included
}

func (self *Filtered) Included() bool { return self.included }

func (self *Filtered) SetRecursive(recursive bool) {
	self.recursive = recursive
}

func (self *Filtered) Recursive() bool { return self.recursive }
