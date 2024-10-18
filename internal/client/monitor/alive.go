package monitor

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dsh2dsh/go-monitoringplugin/v2"

	"github.com/dsh2dsh/zrepl/internal/client/status"
	"github.com/dsh2dsh/zrepl/internal/daemon/job"
	"github.com/dsh2dsh/zrepl/internal/version"
	"github.com/dsh2dsh/zrepl/internal/zfs/zfscmd"
)

func NewAliveCheck(c *status.Client) *AliveCheck {
	check := &AliveCheck{statusClient: c}
	return check.applyOptions()
}

type AliveCheck struct {
	statusClient *status.Client
	resp         *monitoringplugin.Response

	warn time.Duration
	crit time.Duration
}

func (self *AliveCheck) WithThresholds(warn, crit time.Duration) *AliveCheck {
	self.warn = warn
	self.crit = crit
	return self
}

func (self *AliveCheck) applyOptions() *AliveCheck {
	if self.resp == nil {
		self.resp = monitoringplugin.NewResponse("daemon alive")
	}
	return self
}

func (self *AliveCheck) client() *status.Client {
	return self.statusClient
}

func (self *AliveCheck) OutputAndExit() error {
	defer self.resp.OutputAndExit()
	if !self.checkVersions() {
		return nil
	}
	self.checkStatus()
	return nil
}

func (self *AliveCheck) checkVersions() bool {
	daemonVer := self.daemonVersion()
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

func (self *AliveCheck) daemonVersion() string {
	ver, err := self.client().Version()
	if err != nil {
		self.resp.UpdateStatusOnError(
			fmt.Errorf("failed version request: %w", err),
			monitoringplugin.CRITICAL, "", true)
		return ""
	}

	self.resp.UpdateStatus(monitoringplugin.OK, "daemon version: "+ver.String())
	return ver.String()
}

func (self *AliveCheck) checkStatus() bool {
	jobs, activeZFS, err := self.status()
	if err != nil {
		self.resp.UpdateStatusOnError(fmt.Errorf("status: %w", err),
			monitoringplugin.CRITICAL, "", true)
		return false
	}
	return self.checkJobs(jobs) && self.checkActiveZFS(activeZFS)
}

func (self *AliveCheck) status() (map[string]*job.Status,
	[]zfscmd.ActiveCommand, error,
) {
	s, err := self.client().Status()
	if err != nil {
		return nil, nil, fmt.Errorf("failed status request: %w", err)
	}

	m := make(map[string]*job.Status, len(s.Jobs))
	for jname, jstatus := range s.Jobs {
		if jstatus.Type != job.TypeInternal {
			m[jname] = jstatus
		}
	}
	return m, s.Global.ZFSCmds.Active, nil
}

func (self *AliveCheck) checkJobs(jobs map[string]*job.Status) bool {
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
		if d, ok := status.Running(); ok && d > lasting.d {
			lasting.name = jname
			lasting.d = d
		}
	}
	return self.checkLongestJob(lasting.name, lasting.d)
}

func (self *AliveCheck) checkLongestJob(name string, lasting time.Duration,
) bool {
	point := monitoringplugin.NewPerformanceDataPoint(
		"running", lasting.Truncate(time.Second).Seconds()).SetUnit("s")
	point.NewThresholds(0, self.warn.Seconds(),
		0, self.crit.Seconds())
	if err := self.resp.AddPerformanceDataPoint(point); err != nil {
		self.resp.UpdateStatusOnError(err, monitoringplugin.UNKNOWN, "", true)
	} else if lasting > 0 {
		self.resp.UpdateStatus(monitoringplugin.OK, "longest job: "+name)
		self.resp.UpdateStatus(monitoringplugin.OK,
			"running: "+lasting.Truncate(time.Second).String())
	}
	return self.resp.GetStatusCode() == monitoringplugin.OK
}

func (self *AliveCheck) checkActiveZFS(active []zfscmd.ActiveCommand) bool {
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
	point.NewThresholds(0, self.warn.Seconds(), 0,
		self.crit.Seconds())
	if err := self.resp.AddPerformanceDataPoint(point); err != nil {
		self.resp.UpdateStatusOnError(err, monitoringplugin.UNKNOWN, "", true)
	}
	return self.resp.GetStatusCode() == monitoringplugin.OK
}
