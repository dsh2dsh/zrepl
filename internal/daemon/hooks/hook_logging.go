package hooks

import (
	"bufio"
	"bytes"
	"context"
	"sync"

	"github.com/dsh2dsh/zrepl/internal/daemon/logging"
	"github.com/dsh2dsh/zrepl/internal/logger"
)

func getLogger(ctx context.Context) logger.Logger {
	return logging.GetLogger(ctx, logging.SubsysHooks)
}

const MAX_HOOK_LOG_SIZE_DEFAULT int = 1 << 20

type logWriter struct {
	/*
		Mutex prevents:
			concurrent writes to buf, scanner in Write([]byte)
			data race on scanner vs Write([]byte)
				and concurrent write to buf (call to buf.Reset())
				in Close()

		(Also, Close() should generally block until any Write() call completes.)
	*/
	mtx     *sync.Mutex
	buf     bytes.Buffer
	scanner *bufio.Scanner
	logger  logger.Logger
	level   logger.Level
	field   string
}

func NewLogWriter(mtx *sync.Mutex, logger logger.Logger, level logger.Level,
	field string,
) *logWriter {
	w := new(logWriter)
	w.mtx = mtx
	w.scanner = bufio.NewScanner(&w.buf)
	w.logger = logger
	w.level = level
	w.field = field
	return w
}

func (w *logWriter) log(line string) {
	w.logger.WithField(w.field, line).Log(w.level, "hook output")
}

func (w *logWriter) logUnreadBytes() {
	for w.scanner.Scan() {
		w.log(w.scanner.Text())
	}
}

func (w *logWriter) Write(in []byte) (int, error) {
	w.mtx.Lock()
	defer w.mtx.Unlock()

	n, err := w.buf.Write(in)
	if err != nil {
		return n, err
	}
	w.logUnreadBytes()

	// Always reset the scanner for the next Write
	w.buf.Reset()
	w.scanner = bufio.NewScanner(&w.buf)
	return n, nil
}

func (w *logWriter) Close() error {
	w.mtx.Lock()
	defer w.mtx.Unlock()
	w.logUnreadBytes()
	return nil
}
