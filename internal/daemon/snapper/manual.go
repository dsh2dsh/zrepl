package snapper

import (
	"context"
	"time"
)

type manual struct{}

var _ Snapper = (*manual)(nil)

func (self *manual) Cron() string { return "" }

func (self *manual) Periodic() bool { return false }

func (self *manual) Runnable() bool { return false }

func (self *manual) Run(context.Context) {
	// nothing to do
}

func (self *manual) Running() (time.Duration, bool) { return 0, false }

func (self *manual) Report() Report {
	return Report{Type: TypeManual, Manual: &struct{}{}}
}
