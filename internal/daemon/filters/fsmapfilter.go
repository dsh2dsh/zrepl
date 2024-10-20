package filters

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/dsh2dsh/zrepl/internal/endpoint"
	"github.com/dsh2dsh/zrepl/internal/zfs"
)

const (
	MapFilterResultOk   = "ok"
	MapFilterResultOmit = "!"
)

func DatasetMapFilterFromConfig(in map[string]bool,
) (*DatasetMapFilter, error) {
	f := NewDatasetMapFilter(len(in), true)
	for pathPattern, accept := range in {
		mapping := MapFilterResultOmit
		if accept {
			mapping = MapFilterResultOk
		}
		if err := f.Add(pathPattern, mapping); err != nil {
			return nil, fmt.Errorf(
				"invalid mapping entry ['%s':'%s']: %w", pathPattern, mapping, err)
		}
	}
	return f, nil
}

func NewDatasetMapFilter(size int, filterMode bool) *DatasetMapFilter {
	return &DatasetMapFilter{
		entries: make([]datasetMapFilterEntry, 0, size),
	}
}

type DatasetMapFilter struct {
	entries []datasetMapFilterEntry
}

type datasetMapFilterEntry struct {
	path         *zfs.DatasetPath
	mapping      bool
	subtreeMatch bool

	// subtreePattern contains a shell pattern for checking is a subtree matching
	// this definition or not. See pattern syntax in [filepath.Match].
	subtreePattern string
}

func (self *datasetMapFilterEntry) hasPattern() bool {
	return self.subtreePattern != ""
}

func (self *datasetMapFilterEntry) match(path *zfs.DatasetPath) (bool, error) {
	fullPattern := filepath.Join(self.path.ToString(), self.subtreePattern)
	return filepath.Match(fullPattern, path.ToString())
}

func (self *DatasetMapFilter) Add(pathPattern, mapping string) error {
	mappingOk, err := parseDatasetFilterResult(mapping)
	if err != nil {
		return err
	}

	// assert path glob adheres to spec
	const SUBTREE_PATTERN = "<"
	pathStr, pattern, found := strings.Cut(pathPattern, SUBTREE_PATTERN)
	if pattern != "" {
		if strings.Contains(pattern, SUBTREE_PATTERN) {
			return fmt.Errorf(
				"invalid shell pattern %q in path pattern %q: '<' not allowed in shell patterns",
				pattern, pathPattern)
		}
		if _, err := filepath.Match(pattern, ""); err != nil {
			return fmt.Errorf(
				"invalid shell pattern %q in %q: %w", pattern, pathPattern, err)
		}
	}

	path, err := zfs.NewDatasetPath(pathStr)
	if err != nil {
		return fmt.Errorf("pattern is not a dataset path: %s", err)
	}

	self.entries = append(self.entries, datasetMapFilterEntry{
		path:           path,
		mapping:        mappingOk,
		subtreeMatch:   found,
		subtreePattern: pattern,
	})
	return nil
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

// find the most specific prefix mapping we have
//
// longer prefix wins over shorter prefix, direct wins over glob
func (self *DatasetMapFilter) mostSpecificPrefixMapping(path *zfs.DatasetPath,
) (int, bool) {
	lcp, lcp_entry_idx, direct_idx := -1, -1, -1
	for e := range self.entries {
		entry := &self.entries[e]
		ep := self.entries[e].path
		lep := ep.Length()
		switch {
		case !entry.subtreeMatch && ep.Equal(path):
			direct_idx = e
		case entry.subtreeMatch && path.HasPrefix(ep) && lep > lcp:
			lcp = lep
			lcp_entry_idx = e
		}
	}

	switch {
	case direct_idx >= 0:
		return direct_idx, true
	case lcp_entry_idx >= 0:
		return lcp_entry_idx, true
	}
	return 0, false
}

func (self *DatasetMapFilter) Filter(p *zfs.DatasetPath) (bool, error) {
	idx, ok := self.mostSpecificPrefixMapping(p)
	if !ok {
		return false, nil
	}
	entry := &self.entries[idx]

	if entry.hasPattern() {
		if matched, err := entry.match(p); err != nil {
			return false, err
		} else if !matched {
			return false, nil
		}
	}
	return entry.mapping, nil
}

func (self *DatasetMapFilter) UserSpecifiedDatasets() zfs.UserSpecifiedDatasetsSet {
	datasets := make(zfs.UserSpecifiedDatasetsSet)
	for i := range self.entries {
		entry := &self.entries[i]
		datasets[entry.path.ToString()] = true
	}
	return datasets
}

// Creates a new DatasetMapFilter in filter mode from a mapping. All accepting
// mapping results are mapped to accepting filter results. All rejecting mapping
// results are mapped to rejecting filter results.
func (self *DatasetMapFilter) AsFilter() endpoint.FSFilter { return self }
