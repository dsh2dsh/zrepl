package monitor

import (
	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/daemon/filters"
	"github.com/dsh2dsh/zrepl/internal/zfs"
)

func CountRulesFromConfig(in []config.MonitorCount) ([]*CountRule, error) {
	rules := make([]*CountRule, len(in))
	for i := range in {
		r, err := NewCountRule(&in[i])
		if err != nil {
			return nil, err
		}
		rules[i] = r
	}
	return rules, nil
}

func NewCountRule(m *config.MonitorCount) (*CountRule, error) {
	r := &CountRule{MonitorCount: m}
	return r.init()
}

type CountRule struct {
	*config.MonitorCount

	skip *filters.DatasetFilter
}

func (self *CountRule) init() (*CountRule, error) {
	if length := len(self.SkipDatasets); length != 0 {
		skip := filters.New(length)
		if err := skip.AddList(self.SkipDatasets); err != nil {
			return nil, err
		}
		self.skip = skip
	}
	return self, nil
}

func (self *CountRule) Skip(path *zfs.DatasetPath) (bool, error) {
	if self.skip != nil {
		return self.skip.Filter(path)
	}
	return false, nil
}

// --------------------------------------------------

func AgeRulesFromConfig(in []config.MonitorCreation) ([]*AgeRule, error) {
	rules := make([]*AgeRule, len(in))
	for i := range in {
		r, err := NewAgeRule(&in[i])
		if err != nil {
			return nil, err
		}
		rules[i] = r
	}
	return rules, nil
}

func NewAgeRule(m *config.MonitorCreation) (*AgeRule, error) {
	r := &AgeRule{MonitorCreation: m}
	return r.init()
}

type AgeRule struct {
	*config.MonitorCreation

	skip *filters.DatasetFilter
}

func (self *AgeRule) init() (*AgeRule, error) {
	if length := len(self.SkipDatasets); length != 0 {
		skip := filters.New(length)
		if err := skip.AddList(self.SkipDatasets); err != nil {
			return nil, err
		}
		self.skip = skip
	}
	return self, nil
}

func (self *AgeRule) Skip(path *zfs.DatasetPath) (bool, error) {
	if self.skip != nil {
		return self.skip.Filter(path)
	}
	return false, nil
}
