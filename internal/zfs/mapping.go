package zfs

import (
	"context"
	"fmt"
	"slices"

	"github.com/dsh2dsh/zrepl/internal/zfs/zfscmd"
)

type DatasetFilter interface {
	Empty() bool
	Filter(p *DatasetPath) (pass bool, err error)
	// The caller owns the returned set.
	// Implementations should return a copy.
	UserSpecifiedDatasets() UserSpecifiedDatasetsSet
	SingleRecursiveDataset() *DatasetPath
}

// A set of dataset names that the user specified in the configuration file.
type UserSpecifiedDatasetsSet map[string]bool

// Returns a DatasetFilter that does not filter (passes all paths)
func NoFilter() noFilter {
	return noFilter{}
}

type noFilter struct{}

var _ DatasetFilter = noFilter{}

func (noFilter) Filter(p *DatasetPath) (bool, error) {
	return true, nil
}

func (noFilter) UserSpecifiedDatasets() UserSpecifiedDatasetsSet { return nil }

func (noFilter) Empty() bool { return true }

func (noFilter) SingleRecursiveDataset() *DatasetPath { return nil }

func ZFSListMapping(ctx context.Context, filter DatasetFilter,
) ([]*DatasetPath, error) {
	res, err := ZFSListMappingProperties(ctx, filter, nil)
	if err != nil {
		return nil, err
	}
	datasets := make([]*DatasetPath, len(res))
	for i, r := range res {
		datasets[i] = r.Path
	}
	return datasets, nil
}

type ZFSListMappingPropertiesResult struct {
	Path *DatasetPath
	// Guaranteed to have the same length as properties in the originating call
	Fields []string
}

// properties must not contain 'name'
func ZFSListMappingProperties(ctx context.Context, filter DatasetFilter,
	properties []string,
) ([]ZFSListMappingPropertiesResult, error) {
	if filter == nil {
		panic("filter must not be nil")
	} else if slices.Contains(properties, "name") {
		panic("properties must not contain 'name'")
	}

	properties = slices.Concat([]string{"name"}, properties)
	zfsList := ZFSListIter(ctx, properties, nil, "-r", "-t",
		"filesystem,volume")

	unmatchedUserSpecifiedDatasets := filter.UserSpecifiedDatasets()
	datasets := []ZFSListMappingPropertiesResult{}
	for r := range zfsList {
		if r.Err != nil {
			return nil, r.Err
		}
		path, err := NewDatasetPath(r.Fields[0])
		if err != nil {
			return nil, err
		}
		delete(unmatchedUserSpecifiedDatasets, path.ToString())
		if pass, err := filter.Filter(path); err != nil {
			return nil, fmt.Errorf("error calling filter: %w", err)
		} else if pass {
			datasets = append(datasets, ZFSListMappingPropertiesResult{
				Path:   path,
				Fields: r.Fields[1:],
			})
		}
	}

	jobid := zfscmd.GetJobIDOrDefault(ctx, "__nojobid")
	metric := prom.ZFSListUnmatchedUserSpecifiedDatasetCount.
		WithLabelValues(jobid)
	metric.Add(float64(len(unmatchedUserSpecifiedDatasets)))
	return datasets, nil
}
