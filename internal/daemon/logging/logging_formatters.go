package logging

import (
	"context"
	"io"
	"log/slog"
)

const (
	JobField    string = "job"
	SubsysField string = "subsystem"
)

type Formatter interface {
	Enabled(ctx context.Context, level slog.Level) bool
	WithAttrs(attrs []slog.Attr) Formatter
	WithGroup(name string) Formatter
	FormatWithCallback(r slog.Record, cb func(b []byte) error) error
	Write(w io.Writer, r slog.Record) error
}
