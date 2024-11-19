package logger

import (
	"context"
	"log/slog"
)

func NewNullLogger() *Logger {
	return NewLogger(NewOutlets().Add(NewNullOutlet()))
}

func NewNullOutlet() nullOutlet { return nullOutlet{} }

type nullOutlet struct{}

var _ slog.Handler = (*nullOutlet)(nil)

func (nullOutlet) Enabled(context.Context, slog.Level) bool   { return true }
func (nullOutlet) Handle(context.Context, slog.Record) error  { return nil }
func (n nullOutlet) WithAttrs(attrs []slog.Attr) slog.Handler { return n }
func (n nullOutlet) WithGroup(name string) slog.Handler       { return n }
