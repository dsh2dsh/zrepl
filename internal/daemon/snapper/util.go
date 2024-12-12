package snapper

import (
	"context"
	"log/slog"

	"github.com/dsh2dsh/zrepl/internal/daemon/logging"
)

func getLogger(ctx context.Context) *slog.Logger {
	return logging.GetLogger(ctx, logging.SubsysSnapshot)
}

func errOrEmptyString(e error) string {
	if e != nil {
		return e.Error()
	}
	return ""
}
