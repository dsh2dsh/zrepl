package logger

import (
	"log/slog"
)

// The field set by WithError function
const fieldError = "err"

func NewLogger(outlets *Outlets) *slog.Logger { return slog.New(outlets) }

func WithError(l *slog.Logger, err error, msg string) *slog.Logger {
	if err != nil {
		l = l.With(slog.String(fieldError, err.Error()))
	}
	if msg != "" {
		l.Error(msg)
	}
	return l
}
