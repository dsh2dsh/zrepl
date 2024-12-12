package logger_test

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/dsh2dsh/zrepl/internal/logger"
)

func TestLogger(t *testing.T) {
	l := logger.NewTestLogger(t)

	l.Info("foobar")
	l.With(slog.String("fieldname", "fieldval")).Info("log with field")
	logger.WithError(l, errors.New("fooerror"), "error")
}
