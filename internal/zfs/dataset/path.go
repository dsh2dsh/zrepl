package dataset

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

type Path struct {
	comps   []string
	written uint64

	recursive       bool
	recursiveParent *Path

	exclude []*Path
}

func NewPath(s string, opts ...PathOption) (*Path, error) {
	p := new(Path)
	if s == "" {
		p.comps = make([]string, 0)
		return p, nil // the empty dataset path
	}

	const FORBIDDEN = "@#|\t<>*"
	/* Documentation of allowed characters in zfs names:
	https://docs.oracle.com/cd/E19253-01/819-5461/gbcpt/index.html
	Space is missing in the oracle list, but according to
	https://github.com/zfsonlinux/zfs/issues/439
	there is evidence that it was intentionally allowed
	*/
	if strings.ContainsAny(s, FORBIDDEN) {
		return nil, fmt.Errorf(
			"contains forbidden characters (any of '%s')", FORBIDDEN)
	}

	p.comps = strings.Split(s, "/")
	if p.comps[len(p.comps)-1] == "" {
		return nil, errors.New("must not end with a '/'")
	}

	for _, fn := range opts {
		if err := fn(p); err != nil {
			return nil, fmt.Errorf("zfs: parse dataset path %q: %w", s, err)
		}
	}
	return p, nil
}

type PathOption func(p *Path) error

func WithWritten(s string) PathOption {
	return func(p *Path) error { return p.parseWritten(s) }
}

func (self *Path) parseWritten(s string) error {
	written, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return fmt.Errorf("parse 'written' property %q: %w", s, err)
	}
	self.written = written
	return nil
}

func (self *Path) ToString() string {
	return strings.Join(self.comps, "/")
}

func (self *Path) Empty() bool { return len(self.comps) == 0 }

func (self *Path) Extend(extend *Path) {
	self.comps = append(self.comps, extend.comps...)
}

func (self *Path) HasPrefix(prefix *Path) bool {
	if len(prefix.comps) > len(self.comps) {
		return false
	}
	for i := range prefix.comps {
		if prefix.comps[i] != self.comps[i] {
			return false
		}
	}
	return true
}

func (self *Path) TrimPrefix(prefix *Path) {
	if !self.HasPrefix(prefix) {
		return
	}
	prelen := len(prefix.comps)
	newlen := len(self.comps) - prelen
	oldcomps := self.comps
	self.comps = make([]string, newlen)
	for i := range newlen {
		self.comps[i] = oldcomps[prelen+i]
	}
}

func (self *Path) Equal(q *Path) bool {
	if len(self.comps) != len(q.comps) {
		return false
	}
	for i := range self.comps {
		if self.comps[i] != q.comps[i] {
			return false
		}
	}
	return true
}

func (self *Path) Length() int { return len(self.comps) }

func (self *Path) Copy() *Path {
	c := &Path{recursiveParent: self.recursiveParent}
	c.comps = make([]string, len(self.comps))
	copy(c.comps, self.comps)
	return c
}

func (self *Path) MarshalJSON() ([]byte, error) {
	b, err := json.Marshal(self.comps)
	if err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return b, nil
}

func (self *Path) UnmarshalJSON(b []byte) error {
	self.comps = make([]string, 0)
	if err := json.Unmarshal(b, &self.comps); err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	return nil
}

func (self *Path) RecursiveParent() *Path {
	return self.recursiveParent
}

func (self *Path) SetRecursiveParent(parent *Path) {
	self.recursiveParent = parent
}

func (self *Path) Recursive() bool { return self.recursive }

func (self *Path) RecursiveSnapshot() bool {
	return self.recursive && !self.HasExcluded()
}

func (self *Path) WithRecursive(v bool) *Path {
	self.recursive = v
	return self
}

func (self *Path) Written() uint64 { return self.written }

func (self *Path) WithExcluded(p *Path) *Path {
	if !self.Excluded(p) {
		self.exclude = append(self.exclude, p)
	}
	return self
}

func (self *Path) Excluded(p *Path) bool {
	for _, ex := range slices.Backward(self.exclude) {
		if p.HasPrefix(ex) && (p.Equal(ex) || ex.Recursive()) {
			return true
		}
	}
	return false
}

func (self *Path) HasExcluded() bool { return len(self.exclude) != 0 }

func (self *Path) ExcludedString() string {
	n := len(self.exclude)
	switch n {
	case 0:
		return ""
	case 1:
		return self.exclude[0].ToString()
	}

	size := n - 1
	for _, p := range self.exclude {
		size += p.stringLen()
	}

	var sb strings.Builder
	sb.Grow(size)
	for i, p := range self.exclude {
		if i > 0 {
			sb.WriteByte(',')
		}
		p.writeStringTo(&sb)
	}
	return sb.String()
}

func (self *Path) stringLen() int {
	n := len(self.comps)
	switch n {
	case 0:
		return 0
	case 1:
		return len(self.comps[0])
	}

	size := n - 1
	for _, s := range self.comps {
		size += len(s)
	}
	return size
}

func (self *Path) writeStringTo(sb *strings.Builder) {
	for i, s := range self.comps {
		if i > 0 {
			sb.WriteByte('/')
		}
		sb.WriteString(s)
	}
}

func (self *Path) ExcludedPaths() []*Path {
	return self.exclude
}
