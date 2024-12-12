package driver

import (
	"context"
	"log/slog"

	"github.com/dsh2dsh/zrepl/internal/daemon/logging"
)

func getLog(ctx context.Context) *slog.Logger {
	return logging.GetLogger(ctx, logging.SubsysReplication)
}
