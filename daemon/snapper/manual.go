package snapper

import (
	"context"

	"github.com/dsh2dsh/cron/v3"
)

type manual struct{}

func (s *manual) RunPeriodic() bool { return false }

func (s *manual) Run(ctx context.Context, wakeUpCommon chan<- struct{},
	cron *cron.Cron,
) {
	// nothing to do
}

func (s *manual) Report() Report {
	return Report{Type: TypeManual, Manual: &struct{}{}}
}
