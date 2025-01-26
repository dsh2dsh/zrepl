package zfscmd

import (
	"context"
	"log/slog"

	"github.com/dsh2dsh/zrepl/internal/daemon/logging"
)

type contextKey int

const (
	contextKeyJobID contextKey = 1 + iota

	noJobId = "__nojobid"
)

func WithJobID(ctx context.Context, jobID string) context.Context {
	return context.WithValue(ctx, contextKeyJobID, jobID)
}

func GetJobID(ctx context.Context) string {
	ret, ok := ctx.Value(contextKeyJobID).(string)
	if !ok {
		return noJobId
	}
	return ret
}

func getLogger(ctx context.Context) *slog.Logger {
	return logging.GetLogger(ctx, logging.SubsysZFSCmd)
}
