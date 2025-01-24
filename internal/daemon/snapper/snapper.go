package snapper

import (
	"context"
	"fmt"
	"time"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/daemon/filters"
)

type Type string

const (
	TypePeriodic Type = "periodic"
	TypeCron     Type = "cron"
	TypeManual   Type = "manual"
)

type Snapper interface {
	Cron() string
	Periodic() bool
	Runnable() bool
	Run(ctx context.Context)
	Report() Report
	Running() (time.Duration, bool)
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

func (self *Report) Running() (time.Duration, bool) {
	if p := self.Periodic; p != nil {
		return p.Running()
	}
	return 0, false
}

func (self *Report) Cron() string {
	if p := self.Periodic; p != nil {
		return p.CronSpec
	}
	return ""
}

func (self *Report) SleepingUntil() time.Time {
	if p := self.Periodic; p != nil {
		return p.SleepUntil
	}
	return time.Time{}
}

func (self *Report) Progress() (uint64, uint64) {
	if p := self.Periodic; p != nil {
		return p.CompletionProgress()
	}
	return 0, 0
}

func FromConfig(g *config.Global, fsf *filters.DatasetFilter,
	in config.SnapshottingEnum,
) (Snapper, error) {
	switch v := in.Ret.(type) {
	case *config.SnapshottingPeriodic:
		return periodicFromConfig(fsf, v)
	case *config.SnapshottingManual:
		return &manual{}, nil
	default:
		return nil, fmt.Errorf("unknown snapshotting type %T", v)
	}
}
