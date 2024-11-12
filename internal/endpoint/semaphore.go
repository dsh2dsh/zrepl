package endpoint

import (
	"context"
	"io"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/dsh2dsh/zrepl/internal/daemon/logging"
	"github.com/dsh2dsh/zrepl/internal/logger"
)

func NewWeightedReader(ctx context.Context, sem *semaphore.Weighted,
	r io.ReadCloser,
) *WeightedReader {
	return &WeightedReader{ctx: ctx, r: r, sem: sem}
}

type WeightedReader struct {
	ctx context.Context
	r   io.ReadCloser
	sem *semaphore.Weighted

	acquired bool
}

func (self *WeightedReader) Read(p []byte) (n int, err error) {
	n, err = self.r.Read(p)
	switch {
	case err != nil:
		self.release()
	case n > 0 && !self.acquired:
		self.log().WithField("n", n).WithField("p", len(p)).
			Info("waiting for concurrency semaphore")
		err = self.acquire()
	}
	return
}

func (self *WeightedReader) log() logger.Logger {
	return logging.FromContext(self.ctx)
}

func (self *WeightedReader) acquire() error {
	begin := time.Now()
	if err := self.sem.Acquire(self.ctx, 1); err != nil {
		return err
	}
	self.log().WithField("duration", time.Since(begin)).
		Info("acquired concurrency semaphore")
	self.acquired = true
	return nil
}

func (self *WeightedReader) release() {
	if !self.acquired {
		return
	}
	self.log().Debug("release concurrency semaphore")
	self.sem.Release(1)
	self.acquired = false
}

func (self *WeightedReader) Close() error {
	self.release()
	return self.r.Close()
}
