package bytecounter

import (
	"io"
	"sync/atomic"
)

// NewReadCloser wraps rc.
func NewReadCloser(rc io.ReadCloser) *ReadCloser {
	return &ReadCloser{ReadCloser: rc}
}

// ReadCloser wraps an io.ReadCloser, reimplementing its interface and counting
// the bytes written to during copying.
type ReadCloser struct {
	io.ReadCloser
	count uint64
}

var _ io.ReadCloser = (*ReadCloser)(nil)

func (self *ReadCloser) Read(p []byte) (int, error) {
	n, err := self.ReadCloser.Read(p)
	atomic.AddUint64(&self.count, uint64(n))
	return n, err //nolint:wrapcheck // not needed
}

func (self *ReadCloser) Count() uint64 {
	return atomic.LoadUint64(&self.count)
}
