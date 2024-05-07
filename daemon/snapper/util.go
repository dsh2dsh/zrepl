package snapper

import (
	"context"

	"github.com/dsh2dsh/zrepl/daemon/logging"
	"github.com/dsh2dsh/zrepl/logger"
)

type Logger = logger.Logger

func getLogger(ctx context.Context) Logger {
	return logging.GetLogger(ctx, logging.SubsysSnapshot)
}

func errOrEmptyString(e error) string {
	if e != nil {
		return e.Error()
	}
	return ""
}
