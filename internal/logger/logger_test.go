package logger_test

import (
	"errors"
	"testing"
	"time"

	"github.com/kr/pretty"

	"github.com/dsh2dsh/zrepl/internal/logger"
)

type TestOutlet struct {
	Record []logger.Entry
}

func (o *TestOutlet) WriteEntry(entry logger.Entry) error {
	o.Record = append(o.Record, entry)
	return nil
}

func NewTestOutlet() *TestOutlet {
	return &TestOutlet{make([]logger.Entry, 0)}
}

func TestLogger_Basic(t *testing.T) {
	outlet_arr := []logger.Outlet{
		NewTestOutlet(),
		NewTestOutlet(),
	}

	outlets := logger.NewOutlets()
	for _, o := range outlet_arr {
		outlets.Add(o, logger.Debug)
	}

	l := logger.NewLogger(outlets, 1*time.Second)

	l.Info("foobar")

	l.WithField("fieldname", "fieldval").Info("log with field")

	l.WithError(errors.New("fooerror")).Error("error")

	t.Log(pretty.Sprint(outlet_arr))
}
