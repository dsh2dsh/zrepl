package filters

import (
	"fmt"
	"iter"
	"path/filepath"
	"slices"
	"strings"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/zfs"
)

const (
	filterResultOk   = "ok"
	filterResultOmit = "!"
)

func NoFilter() (*DatasetFilter, error) {
	f := New(1)
	err := f.AddList([]config.DatasetFilter{{Recursive: true}})
	if err != nil {
		return nil, err
	}
	return f, nil
}

func NewFromConfig(compatMap map[string]bool, in []config.DatasetFilter,
) (*DatasetFilter, error) {
	f := New(len(compatMap) + len(in))
	if err := f.addMap(compatMap); err != nil {
		return nil, err
	} else if err := f.AddList(in); err != nil {
		return nil, err
	}
	return f, nil
}

func New(size int) *DatasetFilter {
	return &DatasetFilter{entries: make([]*filterItem, 0, size)}
}

type DatasetFilter struct {
	entries     []*filterItem
	parentPaths []*zfs.DatasetPath
}

func (self *DatasetFilter) AddList(in []config.DatasetFilter) error {
	for i := range in {
		entry, err := NewItem(in[i])
		if err != nil {
			return err
		}
		self.append(entry)
	}
	return nil
}

func (self *DatasetFilter) append(e *filterItem) {
	self.appendParent(e)
	self.entries = append(self.entries, e)
}

func (self *DatasetFilter) appendParent(e *filterItem) {
	for _, path := range slices.Backward(self.parentPaths) {
		if e.HasPrefix(path) {
			return
		}
	}

	if p := e.ParentFilesystem(); p != nil {
		self.parentPaths = append(self.parentPaths, p)
	}
}

func (self *DatasetFilter) addMap(in map[string]bool) error {
	for pathPattern, accept := range in {
		if err := self.addCompat(pathPattern, accept); err != nil {
			return fmt.Errorf(
				"invalid mapping entry [%q: %v]: %w", pathPattern, accept, err)
		}
	}
	self.CompatSort()
	return nil
}

func (self *DatasetFilter) addCompat(pathPattern string, mapping bool) error {
	// assert path glob adheres to spec
	const subTreeSep = "<"
	pathStr, pattern, found := strings.Cut(pathPattern, subTreeSep)
	entry, err := NewItem(config.DatasetFilter{
		Pattern:   pathStr,
		Exclude:   !mapping,
		Recursive: found,
	})
	if err != nil {
		return err
	}

	if pattern != "" {
		if strings.Contains(pattern, subTreeSep) {
			return fmt.Errorf(
				"invalid shell pattern %q in path pattern %q: '<' not allowed in shell patterns",
				pattern, pathPattern)
		}
		if !mapping {
			self.append(entry.Clone().WithMapping(true))
		}
		shellPattern := filepath.Join(pathStr, pattern)
		if err := entry.SetShellPattern(shellPattern); err != nil {
			return err
		}
	}

	self.append(entry)
	return nil
}

func (self *DatasetFilter) Add(path, mapping string) error {
	accept, err := parseDatasetFilterResult(mapping)
	if err != nil {
		return err
	}
	return self.addCompat(path, accept)
}

// Parse a dataset filter result
func parseDatasetFilterResult(result string) (bool, error) {
	switch strings.ToLower(result) {
	case filterResultOk:
		return true, nil
	case filterResultOmit:
		return false, nil
	}
	return false, fmt.Errorf("%q is not a valid filter result", result)
}

func (self *DatasetFilter) CompatSort() {
	slices.SortStableFunc(self.entries, func(a, b *filterItem) int {
		return a.CompatCompare(b)
	})

	clear(self.parentPaths)
	self.parentPaths = self.parentPaths[:0]
	for _, e := range self.entries {
		self.appendParent(e)
	}
}

func (self *DatasetFilter) Filter(p *zfs.DatasetPath) (bool, error) {
	_, result, err := self.Filter2(p)
	return result, err
}

func (self *DatasetFilter) Filter2(p *zfs.DatasetPath) (*zfs.DatasetPath, bool,
	error,
) {
	var recursiveRoot *zfs.DatasetPath
	var result bool
	for _, entry := range self.entries {
		if matched, err := entry.Match(p); err != nil {
			return nil, false, err
		} else if matched {
			result = entry.Mapping()
			if r := entry.RecursiveDataset(); r != nil {
				recursiveRoot = r
			}
		}
	}
	return recursiveRoot, result, nil
}

func (self *DatasetFilter) UserSpecifiedDatasets() map[string]bool {
	datasets := make(map[string]bool)
	for i := range self.entries {
		path := self.entries[i].DatasetPath()
		if path != nil {
			datasets[path.ToString()] = true
		}
	}
	return datasets
}

func (self *DatasetFilter) Empty() bool { return len(self.entries) == 0 }

func (self *DatasetFilter) SingleRecursiveDataset() *zfs.DatasetPath {
	if len(self.entries) != 1 {
		return nil
	}
	return self.entries[0].RecursiveDataset()
}

func (self *DatasetFilter) TopFilesystems() (int, iter.Seq[string]) {
	fn := func(yield func(string) bool) {
		for _, p := range self.parentPaths {
			if !yield(p.ToString()) {
				return
			}
		}
	}
	return len(self.parentPaths), fn
}
