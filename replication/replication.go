// Package replication implements replication of filesystems with existing
// versions (snapshots) from a sender to a receiver.
package replication

import (
	"context"

	"github.com/dsh2dsh/zrepl/replication/driver"
)

func Do(ctx context.Context, driverConfig driver.Config,
	planner driver.Planner, running context.Context,
) (driver.ReportFunc, driver.WaitFunc) {
	return driver.Do(ctx, driverConfig, planner, running)
}
