package logger

import (
	"errors"
	"log/slog"
	"testing"
)

func TestLogger(t *testing.T) {
	l := NewTestLogger()

	l.Info("foobar")
	l.With(slog.String("fieldname", "fieldval")).Info("log with field")
	WithError(l, errors.New("fooerror"), "error")
}
