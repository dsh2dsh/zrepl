package filters

import (
	"fmt"
	"path/filepath"
	"slices"
	"strings"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/endpoint"
	"github.com/dsh2dsh/zrepl/internal/zfs"
)

const (
	MapFilterResultOk   = "ok"
	MapFilterResultOmit = "!"
)

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
	entries []*filterItem
}

func (self *DatasetFilter) AddList(in []config.DatasetFilter) error {
	for i := range in {
		configItem := &in[i]
		entry := &filterItem{
			pattern:      configItem.Pattern,
			mapping:      !configItem.Exclude,
			recursive:    configItem.Recursive,
			shellPattern: configItem.Shell,
		}
		if err := entry.Init(); err != nil {
			return err
		}
		self.entries = append(self.entries, entry)
	}
	return nil
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
	entry := &filterItem{
		pattern:   pathStr,
		mapping:   mapping,
		recursive: found,
	}
	if err := entry.Init(); err != nil {
		return err
	}

	if pattern != "" {
		if strings.Contains(pattern, subTreeSep) {
			return fmt.Errorf(
				"invalid shell pattern %q in path pattern %q: '<' not allowed in shell patterns",
				pattern, pathPattern)
		}
		if !mapping {
			rootEntry := entry.Clone()
			rootEntry.mapping = true
			self.entries = append(self.entries, rootEntry)
		}
		entry.pattern = filepath.Join(pathStr, pattern)
		entry.shellPattern = true
		if err := entry.Init(); err != nil {
			return err
		}
	}

	self.entries = append(self.entries, entry)
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
	case MapFilterResultOk:
		return true, nil
	case MapFilterResultOmit:
		return false, nil
	}
	return false, fmt.Errorf("%q is not a valid filter result", result)
}

func (self *DatasetFilter) CompatSort() {
	slices.SortStableFunc(self.entries, func(a, b *filterItem) int {
		return a.CompatCompare(b)
	})
}

func (self *DatasetFilter) Filter(p *zfs.DatasetPath) (bool, error) {
	var lastMapping bool
	for _, entry := range self.entries {
		if matched, err := entry.Match(p); err != nil {
			return false, err
		} else if matched {
			lastMapping = entry.mapping
		}
	}
	return lastMapping, nil
}

func (self *DatasetFilter) UserSpecifiedDatasets() zfs.UserSpecifiedDatasetsSet {
	datasets := make(zfs.UserSpecifiedDatasetsSet)
	for i := range self.entries {
		path := self.entries[i].path
		if path != nil {
			datasets[path.ToString()] = true
		}
	}
	return datasets
}

// Creates a new DatasetMapFilter in filter mode from a mapping. All accepting
// mapping results are mapped to accepting filter results. All rejecting mapping
// results are mapped to rejecting filter results.
func (self *DatasetFilter) AsFilter() endpoint.FSFilter { return self }

func (self *DatasetFilter) Empty() bool { return len(self.entries) == 0 }

func (self *DatasetFilter) SingleRecursiveDataset() *zfs.DatasetPath {
	if len(self.entries) != 1 {
		return nil
	}
	return self.entries[0].RecursiveDataset()
}
