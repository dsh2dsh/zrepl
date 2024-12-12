package hooks

import (
	"bufio"
	"bytes"
	"context"
	"log/slog"

	"github.com/dsh2dsh/zrepl/internal/daemon/logging"
)

func getLogger(ctx context.Context) *slog.Logger {
	return logging.GetLogger(ctx, logging.SubsysHooks)
}

func logOutput(l *slog.Logger, level slog.Level, field string,
	output []byte,
) {
	if len(output) == 0 {
		return
	}

	ctx := context.Background()
	s := bufio.NewScanner(bytes.NewReader(output))
	for s.Scan() {
		l.Log(ctx, level, field+": "+s.Text())
	}
}
