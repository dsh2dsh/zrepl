package hooks

import (
	"bufio"
	"bytes"
	"context"

	"github.com/dsh2dsh/zrepl/internal/daemon/logging"
	"github.com/dsh2dsh/zrepl/internal/logger"
)

func getLogger(ctx context.Context) logger.Logger {
	return logging.GetLogger(ctx, logging.SubsysHooks)
}

func logOutput(l logger.Logger, level logger.Level, field string,
	output []byte,
) {
	if len(output) == 0 {
		return
	}
	s := bufio.NewScanner(bytes.NewReader(output))
	for s.Scan() {
		l.Log(level, field+": "+s.Text())
	}
}
