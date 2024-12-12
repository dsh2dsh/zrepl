package zfscmd

import (
	"context"
	"log/slog"

	"github.com/dsh2dsh/zrepl/internal/daemon/logging"
)

type contextKey int

const (
	contextKeyJobID contextKey = 1 + iota
)

func WithJobID(ctx context.Context, jobID string) context.Context {
	return context.WithValue(ctx, contextKeyJobID, jobID)
}

func GetJobIDOrDefault(ctx context.Context, def string) string {
	return getJobIDOrDefault(ctx, def)
}

func getJobIDOrDefault(ctx context.Context, def string) string {
	ret, ok := ctx.Value(contextKeyJobID).(string)
	if !ok {
		return def
	}
	return ret
}

func getLogger(ctx context.Context) *slog.Logger {
	return logging.GetLogger(ctx, logging.SubsysZFSCmd)
}
