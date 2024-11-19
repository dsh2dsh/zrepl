package endpoint

import (
	"context"

	"github.com/dsh2dsh/zrepl/internal/daemon/logging"
	"github.com/dsh2dsh/zrepl/internal/logger"
)

type contextKey int

const (
	ClientIdentityKey contextKey = iota
)

func getLogger(ctx context.Context) *logger.Logger {
	return logging.GetLogger(ctx, logging.SubsysEndpoint)
}
