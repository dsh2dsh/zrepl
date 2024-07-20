package client

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dsh2dsh/go-monitoringplugin/v2"
	"github.com/spf13/cobra"

	"github.com/dsh2dsh/zrepl/cli"
	"github.com/dsh2dsh/zrepl/client/jsonclient"
	"github.com/dsh2dsh/zrepl/config"
	"github.com/dsh2dsh/zrepl/daemon"
	"github.com/dsh2dsh/zrepl/daemon/filters"
	"github.com/dsh2dsh/zrepl/daemon/job"
	"github.com/dsh2dsh/zrepl/version"
	"github.com/dsh2dsh/zrepl/zfs"
	"github.com/dsh2dsh/zrepl/zfs/zfscmd"
)

const snapshotsOkMsg = "job %q: %s snapshot: %v"

var MonitorCmd = &cli.Subcommand{
	Use:   "monitor",
	Short: "Icinga/Nagios health checks",
	SetupSubcommands: func() []*cli.Subcommand {
		return []*cli.Subcommand{newMonitorAliveCmd(), newMonitorSnapshotsCmd()}
	},
}

func newMonitorAliveCmd() *cli.Subcommand {
	runner := newMonitorAlive()
	return &cli.Subcommand{
		Use:   "alive",
		Short: "check the daemon is alive",
		Run:   runner.run,
		SetupCobra: func(c *cobra.Command) {
			f := c.Flags()
			f.DurationVarP(&runner.warnRunning, "warn", "w", 0,
				"warning job running time")
			f.DurationVarP(&runner.critRunning, "crit", "c", 0,
				"critical job running time")
		},
	}
}

func newMonitorSnapshotsCmd() *cli.Subcommand {
	runner := newMonitorSnapshots()
	return &cli.Subcommand{
		Use:   "snapshots",
		Short: "check snapshots age",
		SetupSubcommands: func() []*cli.Subcommand {
			return []*cli.Subcommand{
				newLatestSnapshotsCmd(runner),
				newOldestSnapshotsCmd(runner),
			}
		},
		SetupCobra: func(c *cobra.Command) {
			f := c.PersistentFlags()
			f.StringVarP(&runner.job, "job", "j", "", "the name of the job")
			f.StringVarP(&runner.prefix, "prefix", "p", "", "snapshot prefix")
			f.DurationVarP(&runner.critical, "crit", "c", 0, "critical snapshot age")
			f.DurationVarP(&runner.warning, "warn", "w", 0, "warning snapshot age")

			_ = c.MarkFlagRequired("job")
			c.MarkFlagsRequiredTogether("prefix", "crit")
		},
	}
}

func newLatestSnapshotsCmd(runner *monitorSnapshots) *cli.Subcommand {
	return &cli.Subcommand{
		Use:   "latest",
		Short: "check latest snapshots are not too old, according to rules",
		Run: func(ctx context.Context, subcmd *cli.Subcommand, args []string,
		) error {
			runner.outputAndExit(runner.run(ctx, subcmd, args))
			return nil
		},
	}
}

func newOldestSnapshotsCmd(runner *monitorSnapshots) *cli.Subcommand {
	return &cli.Subcommand{
		Use:   "oldest",
		Short: "check oldest snapshots are not too old, according to rules",
		Run: func(ctx context.Context, subcmd *cli.Subcommand, args []string,
		) error {
			runner.outputAndExit(runner.withOldest(true).run(ctx, subcmd, args))
			return nil
		},
	}
}

func newMonitorSnapshots() *monitorSnapshots {
	m := &monitorSnapshots{}
	return m.applyOptions()
}

type monitorSnapshots struct {
	job      string
	oldest   bool
	prefix   string
	critical time.Duration
	warning  time.Duration

	resp *monitoringplugin.Response
	age  time.Duration
}

func (self *monitorSnapshots) applyOptions() *monitorSnapshots {
	if self.resp == nil {
		self.resp = monitoringplugin.NewResponse(snapshotsOkMsg)
	}
	return self
}

func (self *monitorSnapshots) withOldest(v bool) *monitorSnapshots {
	self.oldest = v
	return self
}

func (self *monitorSnapshots) run(
	ctx context.Context, subcmd *cli.Subcommand, _ []string,
) error {
	jobConfig, err := subcmd.Config().Job(self.job)
	if err != nil {
		return err
	}

	datasets, rules, err := self.datasetsRules(ctx, jobConfig)
	if err != nil {
		return err
	} else if rules, err = self.overrideRules(rules); err != nil {
		return err
	}
	return self.checkSnapshots(ctx, datasets, rules)
}

func (self *monitorSnapshots) overrideRules(
	rules []config.MonitorSnapshot,
) ([]config.MonitorSnapshot, error) {
	if self.prefix != "" {
		rules = []config.MonitorSnapshot{
			{
				Prefix:   self.prefix,
				Warning:  self.warning,
				Critical: self.critical,
			},
		}
	}

	if len(rules) == 0 {
		return nil, fmt.Errorf(
			"no monitor rules or cli args defined for job %q", self.job)
	}

	return rules, nil
}

func (self *monitorSnapshots) datasetsRules(
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

func (self *monitorSnapshots) datasetsFromFilter(
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

func (self *monitorSnapshots) datasetsFromRootFs(
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

func (self *monitorSnapshots) checkSnapshots(
	ctx context.Context, datasets []string, rules []config.MonitorSnapshot,
) error {
	for _, dataset := range datasets {
		if err := self.checkDataset(ctx, dataset, rules); err != nil {
			return err
		}
	}
	return nil
}

func (self *monitorSnapshots) checkDataset(
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
		case self.age == 0 || d < self.age:
			self.age = d
		}
	}
	return nil
}

func (self *monitorSnapshots) groupSnapshots(
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

func (self *monitorSnapshots) cmpSnapshots(
	new zfs.FilesystemVersion, old zfs.FilesystemVersion,
) bool {
	if self.oldest {
		return new.Creation.Before(old.Creation)
	}
	return new.Creation.After(old.Creation)
}

func (self *monitorSnapshots) outputAndExit(err error) {
	if err != nil {
		self.resp.UpdateStatusOnError(fmt.Errorf("job %q: %w", self.job, err),
			monitoringplugin.UNKNOWN, "", true)
	} else {
		self.resp.WithDefaultOkMessage(fmt.Sprintf(snapshotsOkMsg,
			self.job, self.snapshotType(), self.age))
	}
	self.resp.OutputAndExit()
}

func (self *monitorSnapshots) snapshotType() string {
	if self.oldest {
		return "oldest"
	}
	return "latest"
}

// --------------------------------------------------

func newMonitorAlive() *monitorAlive {
	m := monitorAlive{}
	return m.applyOptions()
}

type monitorAlive struct {
	controlClient *jsonclient.Client
	resp          *monitoringplugin.Response

	warnRunning time.Duration
	critRunning time.Duration
}

func (self *monitorAlive) applyOptions() *monitorAlive {
	if self.resp == nil {
		self.resp = monitoringplugin.NewResponse("daemon alive")
	}
	return self
}

func (self *monitorAlive) client() *jsonclient.Client {
	return self.controlClient
}

func (self *monitorAlive) run(
	ctx context.Context, subcmd *cli.Subcommand, args []string,
) error {
	defer self.resp.OutputAndExit()

	if !self.checkVersions(subcmd.Config().Global.Control.SockPath) {
		return nil
	}
	self.checkStatus()
	return nil
}

func (self *monitorAlive) checkVersions(sockPath string) bool {
	daemonVer := self.daemonVersion(sockPath)
	if daemonVer == "" {
		return false
	}

	clientVer := version.NewZreplVersionInformation().String()
	if clientVer != daemonVer {
		self.resp.UpdateStatus(monitoringplugin.WARNING,
			"client version != daemon version")
		self.resp.UpdateStatus(monitoringplugin.WARNING,
			"client version: "+clientVer)
		return false
	}
	return true
}

func (self *monitorAlive) daemonVersion(sockPath string) string {
	control, err := jsonclient.NewUnix(sockPath)
	if err != nil {
		self.resp.UpdateStatusOnError(fmt.Errorf("new jsonclient: %w", err),
			monitoringplugin.UNKNOWN, "", true)
		return ""
	}
	self.controlClient = control

	var ver version.ZreplVersionInformation
	err = self.client().Post(context.Background(),
		daemon.ControlJobEndpointVersion, nil, &ver)
	if err != nil {
		self.resp.UpdateStatusOnError(
			fmt.Errorf("failed version request: %w", err),
			monitoringplugin.CRITICAL, "", true)
		return ""
	}

	self.resp.UpdateStatus(monitoringplugin.OK, "daemon version: "+ver.String())
	return ver.String()
}

func (self *monitorAlive) checkStatus() bool {
	jobs, activeZFS, err := self.status()
	if err != nil {
		self.resp.UpdateStatusOnError(fmt.Errorf("status: %w", err),
			monitoringplugin.CRITICAL, "", true)
		return false
	}
	return self.checkJobs(jobs) && self.checkActiveZFS(activeZFS)
}

func (self *monitorAlive) status() (map[string]*job.Status,
	[]zfscmd.ActiveCommand, error,
) {
	var s daemon.Status
	err := self.client().Post(context.Background(),
		daemon.ControlJobEndpointStatus, nil, &s)
	if err != nil {
		return nil, nil, fmt.Errorf("failed status request: %w", err)
	}

	m := make(map[string]*job.Status, len(s.Jobs))
	for jname, jstatus := range s.Jobs {
		if !daemon.IsInternalJobName(jname) {
			m[jname] = jstatus
		}
	}
	return m, s.Global.ZFSCmds.Active, nil
}

func (self *monitorAlive) checkJobs(jobs map[string]*job.Status) bool {
	self.resp.WithDefaultOkMessage(strconv.Itoa(len(jobs)) + " jobs")

	lasting := struct {
		name string
		d    time.Duration
	}{}

	for jname, status := range jobs {
		if s := status.Error(); s != "" {
			self.resp.UpdateStatus(monitoringplugin.WARNING, s)
			self.resp.UpdateStatus(monitoringplugin.WARNING, "job: "+jname)
			return false
		}
		if d := status.Running(); d > lasting.d {
			lasting.name = jname
			lasting.d = d
		}
	}
	return self.checkLongestJob(lasting.name, lasting.d)
}

func (self *monitorAlive) checkLongestJob(name string, lasting time.Duration,
) bool {
	point := monitoringplugin.NewPerformanceDataPoint(
		"running", lasting.Truncate(time.Second).Seconds()).SetUnit("s")
	point.NewThresholds(0, self.warnRunning.Seconds(),
		0, self.critRunning.Seconds())
	if err := self.resp.AddPerformanceDataPoint(point); err != nil {
		self.resp.UpdateStatusOnError(err, monitoringplugin.UNKNOWN, "", true)
	} else if lasting > 0 {
		self.resp.UpdateStatus(monitoringplugin.OK, "longest job: "+name)
		self.resp.UpdateStatus(monitoringplugin.OK,
			"running: "+lasting.Truncate(time.Second).String())
	}
	return self.resp.GetStatusCode() == monitoringplugin.OK
}

func (self *monitorAlive) checkActiveZFS(active []zfscmd.ActiveCommand) bool {
	var oldest *zfscmd.ActiveCommand
	for i := range active {
		cmd := &active[i]
		if oldest == nil || cmd.StartedAt.Before(oldest.StartedAt) {
			oldest = cmd
		}
	}

	var d time.Duration
	if oldest != nil {
		d = time.Since(oldest.StartedAt).Truncate(time.Second)
		self.resp.UpdateStatus(monitoringplugin.OK,
			strconv.Itoa(len(active))+" active ZFS commands")
		self.resp.UpdateStatus(monitoringplugin.OK,
			"oldest: "+strings.Join(oldest.Args, " "))
		self.resp.UpdateStatus(monitoringplugin.OK, "running: "+d.String())
	}

	point := monitoringplugin.NewPerformanceDataPoint("zfs", d.Seconds()).
		SetUnit("s")
	point.NewThresholds(0, self.warnRunning.Seconds(), 0,
		self.critRunning.Seconds())
	if err := self.resp.AddPerformanceDataPoint(point); err != nil {
		self.resp.UpdateStatusOnError(err, monitoringplugin.UNKNOWN, "", true)
	}
	return self.resp.GetStatusCode() == monitoringplugin.OK
}
