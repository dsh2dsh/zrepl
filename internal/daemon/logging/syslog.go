package logging

import (
	"context"
	"fmt"
	"log/slog"
	"log/syslog"
	"time"
)

func NewSyslogOutlet(f *SlogFormatter, facility syslog.Priority,
	retryInterval time.Duration,
) *SyslogOutlet {
	return &SyslogOutlet{
		Formatter:     f.WithLogMetadata(false),
		Facility:      facility,
		RetryInterval: retryInterval,
	}
}

type SyslogOutlet struct {
	Formatter          Formatter
	RetryInterval      time.Duration
	Facility           syslog.Priority
	writer             *syslog.Writer
	lastConnectAttempt time.Time
}

var _ slog.Handler = (*SyslogOutlet)(nil)

func (self *SyslogOutlet) Enabled(ctx context.Context, level slog.Level) bool {
	return self.Formatter.Enabled(ctx, level)
}

func (self *SyslogOutlet) Handle(_ context.Context, r slog.Record) error {
	b, err := self.Formatter.Format(r)
	if err != nil {
		return err
	}

	if self.writer == nil {
		now := time.Now()
		if now.Sub(self.lastConnectAttempt) < self.RetryInterval {
			return nil // not an error toward logger
		}
		self.writer, err = syslog.New(self.Facility, "zrepl")
		self.lastConnectAttempt = time.Now()
		if err != nil {
			self.writer = nil
			return fmt.Errorf("new syslog writer: %w", err)
		}
	}

	s := string(b)
	//nolint:wrapcheck // not needed
	switch r.Level {
	case slog.LevelDebug:
		return self.writer.Debug(s)
	case slog.LevelInfo:
		return self.writer.Info(s)
	case slog.LevelWarn:
		return self.writer.Warning(s)
	case slog.LevelError:
		return self.writer.Err(s)
	default:
		// write as error as reaching this case is in fact an error
		return self.writer.Err(s)
	}
}

func (self *SyslogOutlet) WithAttrs(attrs []slog.Attr) slog.Handler {
	o := *self
	o.Formatter = self.Formatter.WithAttrs(attrs)
	return &o
}

func (self *SyslogOutlet) WithGroup(name string) slog.Handler {
	o := *self
	o.Formatter = self.Formatter.WithGroup(name)
	return &o
}
