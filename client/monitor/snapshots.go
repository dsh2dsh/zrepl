package monitor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dsh2dsh/go-monitoringplugin/v2"

	"github.com/dsh2dsh/zrepl/config"
	"github.com/dsh2dsh/zrepl/daemon/filters"
	"github.com/dsh2dsh/zrepl/zfs"
)

func NewSnapCheck(resp *monitoringplugin.Response) *SnapCheck {
	return &SnapCheck{resp: resp}
}

type SnapCheck struct {
	oldest bool
	job    string
	prefix string
	warn   time.Duration
	crit   time.Duration

	resp *monitoringplugin.Response

	age      time.Duration
	snapName string

	datasets        map[string][]zfs.FilesystemVersion
	orderedDatasets []string
}

func (self *SnapCheck) WithPrefix(s string) *SnapCheck {
	self.prefix = s
	return self
}

func (self *SnapCheck) WithThresholds(warn, crit time.Duration) *SnapCheck {
	self.warn = warn
	self.crit = crit
	return self
}

func (self *SnapCheck) WithOldest(v bool) *SnapCheck {
	self.oldest = v
	return self
}

func (self *SnapCheck) WithResponse(resp *monitoringplugin.Response,
) *SnapCheck {
	self.resp = resp
	return self
}

func (self *SnapCheck) UpdateStatus(jobConfig *config.JobEnum) error {
	if err := self.Run(context.Background(), jobConfig); err != nil {
		return err
	} else if self.resp.GetStatusCode() == monitoringplugin.OK {
		self.resp.UpdateStatus(monitoringplugin.OK, self.statusf(
			"%s %q: %v",
			self.snapshotType(), self.snapName, self.age))
	}
	return nil
}

func (self *SnapCheck) Run(ctx context.Context, jobConfig *config.JobEnum,
) error {
	self.job = jobConfig.Name()
	datasets, rules, err := self.datasetRules(ctx, jobConfig)
	if err != nil {
		return err
	} else if rules, err = self.overrideRules(rules); err != nil {
		return err
	}

	for _, dataset := range datasets {
		if err := self.checkDataset(ctx, dataset, rules); err != nil {
			return err
		}
	}
	return nil
}

func (self *SnapCheck) overrideRules(rules []config.MonitorSnapshot,
) ([]config.MonitorSnapshot, error) {
	if self.prefix != "" {
		rules = []config.MonitorSnapshot{
			{
				Prefix:   self.prefix,
				Warning:  self.warn,
				Critical: self.crit,
			},
		}
	}

	if len(rules) == 0 {
		return nil, errors.New("no monitor rules or cli args defined")
	}
	return rules, nil
}

func (self *SnapCheck) datasetRules(
	ctx context.Context, jobConfig *config.JobEnum,
) (datasets []string, rules []config.MonitorSnapshot, err error) {
	switch j := jobConfig.Ret.(type) {
	case *config.PushJob:
		datasets, err = self.datasetsFromFilter(ctx, j.Filesystems)
	case *config.SnapJob:
		datasets, err = self.datasetsFromFilter(ctx, j.Filesystems)
	case *config.SourceJob:
		datasets, err = self.datasetsFromFilter(ctx, j.Filesystems)
	case *config.PullJob:
		datasets, err = self.datasetsFromRootFs(ctx, j.RootFS, 0)
	case *config.SinkJob:
		datasets, err = self.datasetsFromRootFs(ctx, j.RootFS, 1)
	default:
		err = fmt.Errorf("unknown job type %T", j)
	}

	if err != nil {
		return
	}

	cfg := jobConfig.MonitorSnapshots()
	if self.oldest {
		rules = cfg.Oldest
	} else {
		rules = cfg.Latest
	}

	self.datasets = make(map[string][]zfs.FilesystemVersion, len(datasets))
	return
}

func (self *SnapCheck) datasetsFromFilter(
	ctx context.Context, ff config.FilesystemsFilter,
) ([]string, error) {
	if self.orderedDatasets != nil {
		return self.orderedDatasets, nil
	}

	filesystems, err := filters.DatasetMapFilterFromConfig(ff)
	if err != nil {
		return nil, fmt.Errorf("invalid filesystems: %w", err)
	}

	zfsProps, err := zfs.ZFSList(ctx, []string{"name"})
	if err != nil {
		return nil, err
	}

	filtered := []string{}
	for _, item := range zfsProps {
		path, err := zfs.NewDatasetPath(item[0])
		if err != nil {
			return nil, err
		}
		if ok, err := filesystems.Filter(path); err != nil {
			return nil, err
		} else if ok {
			filtered = append(filtered, item[0])
		}
	}

	self.orderedDatasets = filtered
	return filtered, nil
}

func (self *SnapCheck) datasetsFromRootFs(
	ctx context.Context, rootFs string, skipN int,
) ([]string, error) {
	if self.orderedDatasets != nil {
		return self.orderedDatasets, nil
	}

	rootPath, err := zfs.NewDatasetPath(rootFs)
	if err != nil {
		return nil, err
	}

	zfsProps, err := zfs.ZFSList(ctx, []string{"name"}, "-r", rootFs)
	if err != nil {
		return nil, err
	}

	filtered := make([]string, 0, len(zfsProps))
	for _, item := range zfsProps {
		path, err := zfs.NewDatasetPath(item[0])
		if err != nil {
			return nil, err
		} else if path.Length() < rootPath.Length()+1+skipN {
			continue
		}
		if ph, err := zfs.ZFSGetFilesystemPlaceholderState(ctx, path); err != nil {
			return nil, err
		} else if ph.FSExists && !ph.IsPlaceholder {
			filtered = append(filtered, item[0])
		}
	}

	self.orderedDatasets = filtered
	return filtered, nil
}

func (self *SnapCheck) checkDataset(
	ctx context.Context, fsName string, rules []config.MonitorSnapshot,
) error {
	snaps, err := self.snapshots(ctx, fsName)
	if err != nil {
		return err
	}

	latest := self.byCreation(snaps, rules)
	for i := range rules {
		if !self.applyRule(&rules[i], latest[i], fsName) {
			return nil
		}
	}
	return nil
}

func (self *SnapCheck) snapshots(ctx context.Context, fsName string,
) ([]zfs.FilesystemVersion, error) {
	if snaps, ok := self.datasets[fsName]; ok {
		return snaps, nil
	}

	fs, err := zfs.NewDatasetPath(fsName)
	if err != nil {
		return nil, err
	}

	snaps, err := zfs.ZFSListFilesystemVersions(ctx, fs,
		zfs.ListFilesystemVersionsOptions{Types: zfs.Snapshots})
	if err != nil {
		return nil, err
	}
	self.datasets[fsName] = snaps
	return snaps, err
}

func (self *SnapCheck) byCreation(snaps []zfs.FilesystemVersion,
	rules []config.MonitorSnapshot,
) []*zfs.FilesystemVersion {
	grouped := make([]*zfs.FilesystemVersion, len(rules))
	for i := range snaps {
		s := &snaps[i]
		for j := range rules {
			r := &rules[j]
			if r.Prefix == "" || strings.HasPrefix(s.Name, r.Prefix) {
				if grouped[j] == nil || self.cmpSnapshots(s, grouped[j]) {
					grouped[j] = s
				}
				break
			}
		}
	}
	return grouped
}

func (self *SnapCheck) cmpSnapshots(
	newSnap *zfs.FilesystemVersion, oldSnap *zfs.FilesystemVersion,
) bool {
	if self.oldest {
		return newSnap.Creation.Before(oldSnap.Creation)
	}
	return newSnap.Creation.After(oldSnap.Creation)
}

func (self *SnapCheck) snapshotType() string {
	if self.oldest {
		return "oldest"
	}
	return "latest"
}

func (self *SnapCheck) applyRule(rule *config.MonitorSnapshot,
	snap *zfs.FilesystemVersion, fsName string,
) bool {
	if snap == nil && rule.Prefix == "" {
		return true
	} else if snap == nil {
		self.resp.UpdateStatus(monitoringplugin.CRITICAL, fmt.Sprintf(
			"%q has no snapshots with prefix %q", fsName, rule.Prefix))
		return false
	}

	const tooOldFmt = "%s %q too old: %q > %q"
	d := time.Since(snap.Creation).Truncate(time.Second)

	switch {
	case d >= rule.Critical:
		self.resp.UpdateStatus(monitoringplugin.CRITICAL, self.statusf(
			tooOldFmt,
			self.snapshotType(), snap.FullPath(fsName), d, rule.Critical))
		return false
	case rule.Warning > 0 && d >= rule.Warning:
		self.resp.UpdateStatus(monitoringplugin.WARNING, self.statusf(
			tooOldFmt,
			self.snapshotType(), snap.FullPath(fsName), d, rule.Warning))
		return false
	case self.age == 0:
		fallthrough
	case self.oldest && d > self.age:
		fallthrough
	case !self.oldest && d < self.age:
		self.age = d
		self.snapName = snap.Name
	}
	return true
}

func (self *SnapCheck) statusf(format string, a ...any) string {
	return self.status(fmt.Sprintf(format, a...))
}

func (self *SnapCheck) status(s string) string {
	prefix := fmt.Sprintf("job %q", self.job)
	if s == "" {
		return prefix
	}
	return prefix + ": " + s
}

func (self *SnapCheck) Reset() *SnapCheck {
	self.age = 0
	self.snapName = ""
	return self
}
