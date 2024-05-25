package snapper

import (
	"context"
	"fmt"
	"time"

	"github.com/dsh2dsh/cron/v3"

	"github.com/dsh2dsh/zrepl/config"
	"github.com/dsh2dsh/zrepl/zfs"
)

type Type string

const (
	TypePeriodic Type = "periodic"
	TypeCron     Type = "cron"
	TypeManual   Type = "manual"
)

type Snapper interface {
	RunPeriodic() bool
	Run(ctx context.Context, snapshotsTaken chan<- struct{}, cron *cron.Cron)
	Report() Report
}

type Report struct {
	Type     Type
	Periodic *PeriodicReport
	Manual   *struct{}
}

func (self *Report) Error() string {
	if p := self.Periodic; p != nil {
		if p.Error != "" {
			return p.Error
		}
		for _, fs := range p.Progress {
			if fs.HooksHadError {
				return fs.Hooks
			}
		}
	}
	return ""
}

func (self *Report) Running() time.Duration {
	if p := self.Periodic; p != nil {
		return p.Running()
	}
	return 0
}

func FromConfig(g *config.Global, fsf zfs.DatasetFilter, in config.SnapshottingEnum) (Snapper, error) {
	switch v := in.Ret.(type) {
	case *config.SnapshottingPeriodic:
		return periodicFromConfig(fsf, v)
	case *config.SnapshottingManual:
		return &manual{}, nil
	default:
		return nil, fmt.Errorf("unknown snapshotting type %T", v)
	}
}
