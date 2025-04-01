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
	if err := self.initWriter(); err != nil || self.writer == nil {
		return err
	}
	w := self.levelWriter(r.Level)
	return self.Formatter.FormatWithCallback(r,
		func(b []byte) error { return w(string(b)) })
}

func (self *SyslogOutlet) initWriter() error {
	if self.writer != nil {
		return nil
	}

	if !self.lastConnectAttempt.IsZero() {
		if time.Since(self.lastConnectAttempt) < self.RetryInterval {
			return nil // not an error toward logger
		}
	}

	w, err := syslog.New(self.Facility, "zrepl")
	self.lastConnectAttempt = time.Now()
	if err != nil {
		return fmt.Errorf("new syslog writer: %w", err)
	}
	self.writer = w
	return nil
}

func (self *SyslogOutlet) levelWriter(l slog.Level) func(string) error {
	switch l {
	case slog.LevelDebug:
		return self.writer.Debug
	case slog.LevelInfo:
		return self.writer.Info
	case slog.LevelWarn:
		return self.writer.Warning
	case slog.LevelError:
		return self.writer.Err
	}
	// write as error as reaching this case is in fact an error
	return self.writer.Err
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
