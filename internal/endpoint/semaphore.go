package endpoint

import (
	"context"
	"io"

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
		err = self.acquire(n)
	}
	return
}

func (self *WeightedReader) acquire(n int) error {
	log := self.log()
	log.WithField("n", n).Info("waiting for concurrency semaphore")
	defer func() { log.Info("acquired concurrency semaphore") }()

	if err := self.sem.Acquire(self.ctx, 1); err != nil {
		return err
	}

	self.acquired = true
	return nil
}

func (self *WeightedReader) log() logger.Logger {
	return logging.FromContext(self.ctx)
}

func (self *WeightedReader) release() {
	if !self.acquired {
		return
	}
	self.log().Info("release concurrency semaphore")
	self.sem.Release(1)
	self.acquired = false
}

func (self *WeightedReader) Close() error {
	self.release()
	return self.r.Close()
}
