package monitor

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/dsh2dsh/go-monitoringplugin/v2"
	"golang.org/x/sync/errgroup"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/daemon/filters"
	"github.com/dsh2dsh/zrepl/internal/zfs"
)

func NewSnapCheck(resp *monitoringplugin.Response) *SnapCheck {
	check := &SnapCheck{resp: resp}
	return check.WithMaxProcs(0)
}

type SnapCheck struct {
	counts bool
	oldest bool

	job    string
	prefix string
	warn   time.Duration
	crit   time.Duration

	countWarn uint
	countCrit uint

	resp *monitoringplugin.Response

	age       time.Duration
	snapCount uint
	snapName  string
	failed    bool

	datasets        map[*zfs.DatasetPath][]zfs.FilesystemVersion
	orderedDatasets []*zfs.DatasetPath
	maxProcs        int
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

func (self *SnapCheck) WithCounts(v bool) *SnapCheck {
	self.counts = v
	return self
}

func (self *SnapCheck) WithMaxProcs(n int) *SnapCheck {
	if n == 0 {
		n = runtime.GOMAXPROCS(0)
	}
	self.maxProcs = n
	return self
}

func (self *SnapCheck) WithCountThresholds(warn, crit uint) *SnapCheck {
	self.countWarn = warn
	self.countCrit = crit
	return self
}

func (self *SnapCheck) UpdateStatus(jobConfig *config.JobEnum) error {
	if err := self.Run(context.Background(), jobConfig); err != nil {
		return err
	}

	switch {
	case self.failed:
	case self.counts:
		self.updateStatus(monitoringplugin.OK,
			"all snapshots count: %d", self.snapCount)
	default:
		self.updateStatus(monitoringplugin.OK, "%s %q: %v",
			self.snapshotType(), self.snapName, self.age)
	}
	return nil
}

func (self *SnapCheck) Run(ctx context.Context, j *config.JobEnum) error {
	self.job = j.Name()
	if err := self.jobDatasets(ctx, j); err != nil {
		return err
	}

	if self.counts {
		return self.checkCounts(ctx, j)
	}
	return self.checkCreation(ctx, j)
}

func (self *SnapCheck) jobDatasets(ctx context.Context,
	jobConfig *config.JobEnum,
) (err error) {
	if self.orderedDatasets != nil {
		return
	}

	var datasets []*zfs.DatasetPath
	switch j := jobConfig.Ret.(type) {
	case *config.PushJob:
		datasets, err = self.datasetsFromFilter(ctx, j.Filesystems, j.Datasets)
	case *config.SnapJob:
		datasets, err = self.datasetsFromFilter(ctx, j.Filesystems, j.Datasets)
	case *config.SourceJob:
		datasets, err = self.datasetsFromFilter(ctx, j.Filesystems, j.Datasets)
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

	slices.SortFunc(datasets, func(a, b *zfs.DatasetPath) int {
		return strings.Compare(a.ToString(), b.ToString())
	})
	self.orderedDatasets = datasets
	self.datasets = make(map[*zfs.DatasetPath][]zfs.FilesystemVersion,
		len(datasets))
	return self.preloadSnapshots(ctx)
}

func (self *SnapCheck) datasetsFromFilter(
	ctx context.Context, ff config.FilesystemsFilter,
	df []config.DatasetFilter,
) ([]*zfs.DatasetPath, error) {
	filesystems, err := filters.NewFromConfig(ff, df)
	if err != nil {
		return nil, fmt.Errorf("invalid filesystems: %w", err)
	}

	filtered := []*zfs.DatasetPath{}
	for fields, err := range zfs.ZFSListIter(ctx, []string{"name"}, nil) {
		if err != nil {
			return nil, err
		} else if path, err := zfs.NewDatasetPath(fields[0]); err != nil {
			return nil, err
		} else if ok, err := filesystems.Filter(path); err != nil {
			return nil, err
		} else if ok {
			filtered = append(filtered, path)
		}
	}
	return filtered, nil
}

func (self *SnapCheck) datasetsFromRootFs(
	ctx context.Context, rootFs string, skipN int,
) ([]*zfs.DatasetPath, error) {
	rootPath, err := zfs.NewDatasetPath(rootFs)
	if err != nil {
		return nil, err
	}

	propsByFS, err := zfs.ZFSGetRecursive(ctx,
		rootFs, -1, []string{"filesystem", "volume"},
		[]string{zfs.PlaceholderPropertyName}, zfs.SourceAny)
	if err != nil {
		return nil, fmt.Errorf("properties of %q: %w", rootFs, err)
	}

	filtered := make([]*zfs.DatasetPath, 0, len(propsByFS))
	for fs, props := range propsByFS {
		path, err := zfs.NewDatasetPath(fs)
		if err != nil {
			return nil, err
		} else if path.Length() < rootPath.Length()+1+skipN {
			continue
		}
		p := props.GetDetails(zfs.PlaceholderPropertyName)
		if p.Source == zfs.SourceLocal && p.Value == "on" {
			continue
		}
		filtered = append(filtered, path)
	}
	return filtered, nil
}

func (self *SnapCheck) preloadSnapshots(ctx context.Context,
) error {
	var mu sync.Mutex
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(self.maxProcs)

	for _, dataset := range self.orderedDatasets {
		if ctx.Err() != nil {
			break
		}
		g.Go(func() error {
			snapshots, err := zfsListSnapshots(ctx, dataset)
			if err != nil {
				return err
			}
			mu.Lock()
			self.datasets[dataset] = snapshots
			mu.Unlock()
			return nil
		})
	}
	return g.Wait() //nolint:wrapcheck // our error
}

func zfsListSnapshots(ctx context.Context, fs *zfs.DatasetPath,
) ([]zfs.FilesystemVersion, error) {
	snaps, err := zfs.ZFSListFilesystemVersions(ctx, fs,
		zfs.ListFilesystemVersionsOptions{Types: zfs.Snapshots})
	if err != nil {
		return nil, err
	}
	return snaps, err
}

func (self *SnapCheck) checkCounts(ctx context.Context, j *config.JobEnum,
) error {
	configRules, err := self.overrideCountRules(j)
	if err != nil {
		return err
	}

	rules, err := CountRulesFromConfig(configRules)
	if err != nil {
		return err
	}

	for _, dataset := range self.orderedDatasets {
		if err := self.checkSnapsCounts(ctx, dataset, rules); err != nil {
			return err
		}
	}
	return nil
}

func (self *SnapCheck) overrideCountRules(j *config.JobEnum,
) ([]config.MonitorCount, error) {
	if self.prefix != "" {
		return []config.MonitorCount{
			{
				Prefix:   self.prefix,
				Warning:  self.countWarn,
				Critical: self.countCrit,
			},
		}, nil
	}

	rules := j.MonitorSnapshots().Count
	if len(rules) == 0 {
		return nil, errors.New("no monitor rules or cli args defined")
	}
	return rules, nil
}

func (self *SnapCheck) checkSnapsCounts(ctx context.Context,
	path *zfs.DatasetPath, rules []*CountRule,
) error {
	snapshots, err := self.snapshots(ctx, path)
	if err != nil {
		return err
	}

	prefixes := make([]string, len(rules))
	for i := range rules {
		prefixes[i] = rules[i].Prefix
	}
	grouped := groupSnapshots(snapshots, prefixes)

	for i := range rules {
		if !self.applyCountRule(rules[i], path, &grouped[i]) {
			break
		}
	}
	return nil
}

func (self *SnapCheck) snapshots(ctx context.Context, path *zfs.DatasetPath,
) ([]zfs.FilesystemVersion, error) {
	if snaps, ok := self.datasets[path]; ok {
		return snaps, nil
	}

	snaps, err := zfsListSnapshots(ctx, path)
	if err != nil {
		return nil, err
	}
	self.datasets[path] = snaps
	return snaps, err
}

func groupSnapshots(snapshots []zfs.FilesystemVersion, prefixes []string,
) []groupItem {
	grouped := make([]groupItem, len(prefixes))
	for i := range snapshots {
		s := &snapshots[i]
		for j, p := range prefixes {
			if p == "" || strings.HasPrefix(s.Name, p) {
				g := &grouped[j]
				g.Count++
				if g.Oldest == nil || s.Creation.Before(g.Oldest.Creation) {
					g.Oldest = s
				}
				if g.Latest == nil || s.Creation.After(g.Latest.Creation) {
					g.Latest = s
				}
				break
			}
		}
	}
	return grouped
}

type groupItem struct {
	Count  uint
	Oldest *zfs.FilesystemVersion
	Latest *zfs.FilesystemVersion
}

func (self *groupItem) Snapshot(oldest bool) *zfs.FilesystemVersion {
	if oldest {
		return self.Oldest
	}
	return self.Latest
}

func (self *SnapCheck) applyCountRule(rule *CountRule, path *zfs.DatasetPath,
	g *groupItem,
) bool {
	if skip, err := rule.Skip(path); err != nil {
		self.resp.UpdateStatus(monitoringplugin.UNKNOWN, fmt.Sprintf(
			"error skipping dataset %q: %s", path.ToString(), err))
		return false
	} else if skip {
		return true
	}

	if g.Count == 0 && rule.Prefix == "" {
		return true
	} else if g.Count == 0 {
		self.resp.UpdateStatus(monitoringplugin.CRITICAL, fmt.Sprintf(
			"%q has no snapshots with prefix %q", path.ToString(), rule.Prefix))
		return false
	}

	const msg = "%s: %q snapshots count: %d (%d)"
	switch {
	case g.Count >= rule.Critical:
		self.updateStatus(monitoringplugin.CRITICAL, msg,
			path.ToString(), rule.Prefix, g.Count, rule.Critical)
		return false
	case rule.Warning > 0 && g.Count >= rule.Warning:
		self.updateStatus(monitoringplugin.WARNING, msg,
			path.ToString(), rule.Prefix, g.Count, rule.Warning)
		return false
	default:
		self.snapCount += g.Count
	}
	return true
}

func (self *SnapCheck) checkCreation(ctx context.Context, j *config.JobEnum,
) error {
	configRules, err := self.overrideAgeRules(self.rulesByCreation(j))
	if err != nil {
		return err
	}

	rules, err := AgeRulesFromConfig(configRules)
	if err != nil {
		return err
	}

	for _, dataset := range self.orderedDatasets {
		if err := self.checkSnapsCreation(ctx, dataset, rules); err != nil {
			return err
		}
	}
	return nil
}

func (self *SnapCheck) overrideAgeRules(rules []config.MonitorCreation,
) ([]config.MonitorCreation, error) {
	if self.prefix != "" {
		return []config.MonitorCreation{
			{
				Prefix:   self.prefix,
				Warning:  self.warn,
				Critical: self.crit,
			},
		}, nil
	}

	if len(rules) == 0 {
		return nil, errors.New("no monitor rules or cli args defined")
	}
	return rules, nil
}

func (self *SnapCheck) rulesByCreation(j *config.JobEnum,
) []config.MonitorCreation {
	cfg := j.MonitorSnapshots()
	if self.oldest {
		return cfg.Oldest
	}
	return cfg.Latest
}

func (self *SnapCheck) checkSnapsCreation(ctx context.Context,
	path *zfs.DatasetPath, rules []*AgeRule,
) error {
	snapshots, err := self.snapshots(ctx, path)
	if err != nil {
		return err
	}

	prefixes := make([]string, len(rules))
	for i := range rules {
		prefixes[i] = rules[i].Prefix
	}
	grouped := groupSnapshots(snapshots, prefixes)

	for i := range rules {
		s := grouped[i].Snapshot(self.oldest)
		if !self.applyCreationRule(rules[i], s, path) {
			return nil
		}
	}
	return nil
}

func (self *SnapCheck) applyCreationRule(rule *AgeRule,
	snap *zfs.FilesystemVersion, path *zfs.DatasetPath,
) bool {
	if skip, err := rule.Skip(path); err != nil {
		self.resp.UpdateStatus(monitoringplugin.UNKNOWN, fmt.Sprintf(
			"error skipping dataset %q: %s", path.ToString(), err))
		return false
	} else if skip {
		return true
	}

	if snap == nil && rule.Prefix == "" {
		return true
	} else if snap == nil {
		self.resp.UpdateStatus(monitoringplugin.CRITICAL, fmt.Sprintf(
			"%q has no snapshots with prefix %q", path.ToString(), rule.Prefix))
		return false
	}

	const tooOldFmt = "%s %q too old: %q > %q"
	d := time.Since(snap.Creation).Truncate(time.Second)

	switch {
	case d >= rule.Critical:
		self.updateStatus(monitoringplugin.CRITICAL, tooOldFmt,
			self.snapshotType(), snap.FullPath(path.ToString()), d, rule.Critical)
		return false
	case rule.Warning > 0 && d >= rule.Warning:
		self.updateStatus(monitoringplugin.WARNING, tooOldFmt,
			self.snapshotType(), snap.FullPath(path.ToString()), d, rule.Warning)
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

func (self *SnapCheck) updateStatus(statusCode int, format string, a ...any) {
	var statusMessage string
	if len(a) == 0 {
		statusMessage = fmt.Sprintf("job %q: %s", self.job, format)
	} else {
		statusMessage = fmt.Sprintf("job %q: ", self.job) +
			fmt.Sprintf(format, a...)
	}
	self.updateResponse(statusCode, statusMessage)
}

func (self *SnapCheck) updateResponse(statusCode int, statusMessage string) {
	self.failed = self.failed || statusCode != monitoringplugin.OK
	self.resp.UpdateStatus(statusCode, statusMessage)
}

func (self *SnapCheck) snapshotType() string {
	if self.oldest {
		return "oldest"
	}
	return "latest"
}

func (self *SnapCheck) Reset() *SnapCheck {
	self.age = 0
	self.snapCount = 0
	self.snapName = ""
	self.failed = false
	return self
}
