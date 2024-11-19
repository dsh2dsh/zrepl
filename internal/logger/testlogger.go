package logger

import (
	"io"
	"log/slog"
	"testing"
)

func NewTestLogger(t *testing.T) *Logger {
	h := slog.NewTextHandler(&testingWriter{t}, nil)
	return NewLogger(NewOutlets().Add(h))
}

type testingWriter struct {
	t *testing.T
}

var _ io.Writer = (*testingWriter)(nil)

func (self *testingWriter) Write(p []byte) (int, error) {
	self.t.Log(string(p))
	return len(p), nil
}
