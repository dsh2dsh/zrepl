package zfs

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

type DatasetPath struct {
	comps   []string
	written uint64

	recursive       bool
	recursiveParent *DatasetPath

	exclude []*DatasetPath
}

func NewDatasetPath(s string, opts ...DatasetPathOption) (*DatasetPath, error) {
	p := new(DatasetPath)
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

type DatasetPathOption func(p *DatasetPath) error

func WithWritten(s string) DatasetPathOption {
	return func(p *DatasetPath) error { return p.parseWritten(s) }
}

func (self *DatasetPath) parseWritten(s string) error {
	written, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return fmt.Errorf("parse 'written' property %q: %w", s, err)
	}
	self.written = written
	return nil
}

func (self *DatasetPath) ToString() string {
	return strings.Join(self.comps, "/")
}

func (self *DatasetPath) Empty() bool { return len(self.comps) == 0 }

func (self *DatasetPath) Extend(extend *DatasetPath) {
	self.comps = append(self.comps, extend.comps...)
}

func (self *DatasetPath) HasPrefix(prefix *DatasetPath) bool {
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

func (self *DatasetPath) TrimPrefix(prefix *DatasetPath) {
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

func (self *DatasetPath) Equal(q *DatasetPath) bool {
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

func (self *DatasetPath) Length() int { return len(self.comps) }

func (self *DatasetPath) Copy() *DatasetPath {
	c := &DatasetPath{recursiveParent: self.recursiveParent}
	c.comps = make([]string, len(self.comps))
	copy(c.comps, self.comps)
	return c
}

func (self *DatasetPath) MarshalJSON() ([]byte, error) {
	b, err := json.Marshal(self.comps)
	if err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return b, nil
}

func (self *DatasetPath) UnmarshalJSON(b []byte) error {
	self.comps = make([]string, 0)
	if err := json.Unmarshal(b, &self.comps); err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	return nil
}

func (self *DatasetPath) RecursiveParent() *DatasetPath {
	return self.recursiveParent
}

func (self *DatasetPath) SetRecursiveParent(parent *DatasetPath) {
	self.recursiveParent = parent
}

func (self *DatasetPath) Recursive() bool { return self.recursive }

func (self *DatasetPath) RecursiveSnapshot() bool {
	return self.recursive && !self.HasExcluded()
}

func (self *DatasetPath) SetRecursive() { self.recursive = true }

func (self *DatasetPath) Written() uint64 { return self.written }

func (self *DatasetPath) WithExcluded(p *DatasetPath) *DatasetPath {
	if !self.Excluded(p) {
		self.exclude = append(self.exclude, p)
	}
	return self
}

func (self *DatasetPath) Excluded(p *DatasetPath) bool {
	for _, ex := range slices.Backward(self.exclude) {
		if p.HasPrefix(ex) {
			return true
		}
	}
	return false
}

func (self *DatasetPath) HasExcluded() bool { return len(self.exclude) != 0 }

func (self *DatasetPath) ExcludedString() string {
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

func (self *DatasetPath) stringLen() int {
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

func (self *DatasetPath) writeStringTo(sb *strings.Builder) {
	for i, s := range self.comps {
		if i > 0 {
			sb.WriteByte('/')
		}
		sb.WriteString(s)
	}
}
