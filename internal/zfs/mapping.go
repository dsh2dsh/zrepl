package zfs

import (
	"context"
	"fmt"

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

// --------------------------------------------------

func ZFSListMapping(ctx context.Context, filter DatasetFilter,
) ([]*DatasetPath, error) {
	props := []string{"name"}
	cmd := NewListCmd(ctx, props, []string{"-r", "-t", "filesystem,volume"})
	v, err, _ := sg.Do(cmd.String(), func() (any, error) {
		datasets, err := listDatasets(ctx, props, nil, cmd)
		if err != nil {
			return nil, err
		}
		return &datasets, nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // already wrapped
	}
	allDatasets := v.(*[]*DatasetPath)

	unmatchedUserSpecifiedDatasets := filter.UserSpecifiedDatasets()
	datasets := []*DatasetPath{}
	for _, path := range *allDatasets {
		delete(unmatchedUserSpecifiedDatasets, path.ToString())
		if ok, err := filter.Filter(path); err != nil {
			return nil, fmt.Errorf("error calling filter: %w", err)
		} else if ok {
			datasets = append(datasets, path)
		}
	}

	jobid := zfscmd.GetJobIDOrDefault(ctx, "__nojobid")
	metric := prom.ZFSListUnmatchedUserSpecifiedDatasetCount.
		WithLabelValues(jobid)
	metric.Add(float64(len(unmatchedUserSpecifiedDatasets)))
	return datasets, nil
}
