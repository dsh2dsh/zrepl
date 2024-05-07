package endpoint

import (
	"context"

	"github.com/dsh2dsh/zrepl/daemon/logging"
	"github.com/dsh2dsh/zrepl/logger"
)

type contextKey int

const (
	ClientIdentityKey contextKey = iota
)

type Logger = logger.Logger

func getLogger(ctx context.Context) Logger {
	return logging.GetLogger(ctx, logging.SubsysEndpoint)
}
