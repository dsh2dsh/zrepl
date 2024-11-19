package logger

import (
	"log/slog"
)

const (
	// The field set by WithError function
	FieldError = "err"
)

func WithError(l *slog.Logger, err error, msg string) *slog.Logger {
	if err != nil {
		l = l.With(slog.String(FieldError, err.Error()))
	}
	if msg != "" {
		l.Error(msg)
	}
	return l
}

type Logger struct {
	*slog.Logger
}

func NewLogger(outlets *Outlets) *Logger {
	return &Logger{slog.New(outlets)}
}

func (self *Logger) WithField(field string, val any) *Logger {
	return &Logger{self.With(slog.Any(field, val))}
}

func (self *Logger) WithError(err error) *Logger {
	if err == nil {
		return self
	}
	return &Logger{self.With(slog.String(FieldError, err.Error()))}
}
