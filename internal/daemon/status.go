package daemon

import (
	"github.com/dsh2dsh/zrepl/internal/daemon/job"
	"github.com/dsh2dsh/zrepl/internal/util/envconst"
	"github.com/dsh2dsh/zrepl/internal/zfs/zfscmd"
)

type Status struct {
	Jobs   map[string]*job.Status
	Global GlobalStatus
}

type GlobalStatus struct {
	ZFSCmds   *zfscmd.Report
	Envconst  *envconst.Report
	OsEnviron []string
}

func (self *Status) JobCounts() (running, withErr int) {
	for _, j := range self.Jobs {
		if j.Type == job.TypeInternal {
			continue
		}
		if _, ok := j.Running(); ok {
			running++
		}
		if j.Error() != "" {
			withErr++
		}
	}
	return
}
