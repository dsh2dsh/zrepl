package endpoint

import (
	"context"
	"log/slog"

	"github.com/dsh2dsh/zrepl/internal/daemon/logging"
)

type contextKey int

const (
	ClientIdentityKey contextKey = iota
)

func getLogger(ctx context.Context) *slog.Logger {
	return logging.GetLogger(ctx, logging.SubsysEndpoint)
}
