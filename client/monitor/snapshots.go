package monitor

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dsh2dsh/go-monitoringplugin/v2"

	"github.com/dsh2dsh/zrepl/config"
	"github.com/dsh2dsh/zrepl/daemon/filters"
	"github.com/dsh2dsh/zrepl/zfs"
)

const snapshotsOkMsg = "job %q: %s snapshot: %q (%v)"

func NewSnapCheck() *SnapCheck {
	check := &SnapCheck{}
	return check.applyOptions()
}

type SnapCheck struct {
	oldest bool

	job    string
	prefix string

	warn time.Duration
	crit time.Duration

	resp *monitoringplugin.Response

	age      time.Duration
	snapName string
}

func (self *SnapCheck) applyOptions() *SnapCheck {
	if self.resp == nil {
		self.resp = monitoringplugin.NewResponse(snapshotsOkMsg)
	}
	return self
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

func (self *SnapCheck) OutputAndExit(ctx context.Context,
	jobConfig *config.JobEnum,
) error {
	if err := self.run(ctx, jobConfig); err != nil {
		self.resp.UpdateStatusOnError(fmt.Errorf("job %q: %w", self.job, err),
			monitoringplugin.UNKNOWN, "", true)
	} else {
		self.resp.WithDefaultOkMessage(fmt.Sprintf(snapshotsOkMsg,
			self.job, self.snapshotType(), self.snapName, self.age))
	}
	self.resp.OutputAndExit()
	return nil
}

func (self *SnapCheck) run(ctx context.Context, jobConfig *config.JobEnum,
) error {
	self.job = jobConfig.Name()
	datasets, rules, err := self.datasetsRules(ctx, jobConfig)
	if err != nil {
		return err
	} else if rules, err = self.overrideRules(rules); err != nil {
		return err
	}
	return self.checkSnapshots(ctx, datasets, rules)
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
		return nil, fmt.Errorf(
			"no monitor rules or cli args defined for job %q", self.job)
	}

	return rules, nil
}

func (self *SnapCheck) datasetsRules(
	ctx context.Context, jobConfig *config.JobEnum,
) (datasets []string, rules []config.MonitorSnapshot, err error) {
	var cfg config.MonitorSnapshots
	switch job := jobConfig.Ret.(type) {
	case *config.PushJob:
		cfg = job.MonitorSnapshots
		datasets, err = self.datasetsFromFilter(ctx, job.Filesystems)
	case *config.SnapJob:
		cfg = job.MonitorSnapshots
		datasets, err = self.datasetsFromFilter(ctx, job.Filesystems)
	case *config.SourceJob:
		cfg = job.MonitorSnapshots
		datasets, err = self.datasetsFromFilter(ctx, job.Filesystems)
	case *config.PullJob:
		cfg = job.MonitorSnapshots
		datasets, err = self.datasetsFromRootFs(ctx, job.RootFS, 0)
	case *config.SinkJob:
		cfg = job.MonitorSnapshots
		datasets, err = self.datasetsFromRootFs(ctx, job.RootFS, 1)
	default:
		err = fmt.Errorf("unknown job type %T", job)
	}

	if err == nil {
		if self.oldest {
			rules = cfg.Oldest
		} else {
			rules = cfg.Latest
		}
	}

	return
}

func (self *SnapCheck) datasetsFromFilter(
	ctx context.Context, ff config.FilesystemsFilter,
) ([]string, error) {
	filesystems, err := filters.DatasetMapFilterFromConfig(ff)
	if err != nil {
		return nil, fmt.Errorf("job %q has invalid filesystems: %w", self.job, err)
	}

	zfsProps, err := zfs.ZFSList(ctx, []string{"name"})
	if err != nil {
		return nil, err
	}

	filtered := make([]string, 0, len(zfsProps))
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

	return filtered, nil
}

func (self *SnapCheck) datasetsFromRootFs(
	ctx context.Context, rootFs string, skipN int,
) ([]string, error) {
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

	return filtered, nil
}

func (self *SnapCheck) checkSnapshots(
	ctx context.Context, datasets []string, rules []config.MonitorSnapshot,
) error {
	for _, dataset := range datasets {
		if err := self.checkDataset(ctx, dataset, rules); err != nil {
			return err
		}
	}
	return nil
}

func (self *SnapCheck) checkDataset(
	ctx context.Context, name string, rules []config.MonitorSnapshot,
) error {
	path, err := zfs.NewDatasetPath(name)
	if err != nil {
		return err
	}

	snaps, err := zfs.ZFSListFilesystemVersions(ctx, path,
		zfs.ListFilesystemVersionsOptions{Types: zfs.Snapshots})
	if err != nil {
		return err
	}

	latest := self.groupSnapshots(snaps, rules)
	for i, rule := range rules {
		d := time.Since(latest[i].Creation).Truncate(time.Second)
		const tooOldFmt = "%s %q too old: %q > %q"
		switch {
		case rule.Prefix == "" && latest[i].Creation.IsZero():
		case latest[i].Creation.IsZero():
			self.resp.UpdateStatus(monitoringplugin.CRITICAL, fmt.Sprintf(
				"%q has no snapshots with prefix %q", name, rule.Prefix))
			return nil
		case d >= rule.Critical:
			self.resp.UpdateStatus(monitoringplugin.CRITICAL, fmt.Sprintf(
				tooOldFmt,
				self.snapshotType(), latest[i].FullPath(name), d, rule.Critical))
			return nil
		case rule.Warning > 0 && d >= rule.Warning:
			self.resp.UpdateStatus(monitoringplugin.WARNING, fmt.Sprintf(
				tooOldFmt,
				self.snapshotType(), latest[i].FullPath(name), d, rule.Warning))
			return nil
		case self.age == 0:
			fallthrough
		case self.oldest && d > self.age:
			fallthrough
		case !self.oldest && d < self.age:
			self.age = d
			self.snapName = latest[i].Name
		}
	}
	return nil
}

func (self *SnapCheck) groupSnapshots(
	snaps []zfs.FilesystemVersion, rules []config.MonitorSnapshot,
) []zfs.FilesystemVersion {
	latest := make([]zfs.FilesystemVersion, len(rules))
	unknownSnaps := snaps[:0]

	for i, rule := range rules {
		for _, snap := range snaps {
			if rule.Prefix == "" || strings.HasPrefix(snap.GetName(), rule.Prefix) {
				if latest[i].Creation.IsZero() || self.cmpSnapshots(snap, latest[i]) {
					latest[i] = snap
				}
			} else {
				unknownSnaps = append(unknownSnaps, snap)
			}
		}
		snaps = unknownSnaps
		unknownSnaps = snaps[:0]
		if len(snaps) == 0 {
			break
		}
	}
	return latest
}

func (self *SnapCheck) cmpSnapshots(
	new zfs.FilesystemVersion, old zfs.FilesystemVersion,
) bool {
	if self.oldest {
		return new.Creation.Before(old.Creation)
	}
	return new.Creation.After(old.Creation)
}

func (self *SnapCheck) snapshotType() string {
	if self.oldest {
		return "oldest"
	}
	return "latest"
}
