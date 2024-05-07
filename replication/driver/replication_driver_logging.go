package driver

import (
	"context"

	"github.com/dsh2dsh/zrepl/daemon/logging"
	"github.com/dsh2dsh/zrepl/logger"
)

func getLog(ctx context.Context) logger.Logger {
	return logging.GetLogger(ctx, logging.SubsysReplication)
}
