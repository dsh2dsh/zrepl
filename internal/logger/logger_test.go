package logger_test

import (
	"errors"
	"testing"

	"github.com/dsh2dsh/zrepl/internal/logger"
)

func TestLogger(t *testing.T) {
	l := logger.NewTestLogger(t)

	l.Info("foobar")
	l.WithField("fieldname", "fieldval").Info("log with field")
	l.WithError(errors.New("fooerror")).Error("error")
}
