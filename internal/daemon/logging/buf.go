package logging

import (
	"bytes"
	"sync"
)

var bufPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 0, 1024))
	},
}

func NewBuffer() *Buffer { return new(Buffer) }

type Buffer struct {
	*bytes.Buffer
}

func (self *Buffer) Alloc() { self.Buffer = bufPool.Get().(*bytes.Buffer) }

func (self *Buffer) Free() {
	// To reduce peak allocation, return only smaller buffers to the pool.
	const maxBufferSize = 16 << 10
	if self.Cap() <= maxBufferSize {
		self.Reset()
		bufPool.Put(self.Buffer)
	}
	self.Buffer = nil
}
