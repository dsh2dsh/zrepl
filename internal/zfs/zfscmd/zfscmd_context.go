package zfscmd

import (
	"context"

	"github.com/dsh2dsh/zrepl/internal/daemon/logging"
	"github.com/dsh2dsh/zrepl/internal/logger"
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

func getLogger(ctx context.Context) *logger.Logger {
	return logging.GetLogger(ctx, logging.SubsysZFSCmd)
}
