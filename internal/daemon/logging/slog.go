package logging

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"sync"
	"unicode"

	"github.com/dsh2dsh/zrepl/internal/config"
)

func parseSlogFormatter(in *config.LoggingOutletCommon) *SlogFormatter {
	return NewSlogFormatter().
		WithHideFields(in.HideFields).
		WithLogTime(in.Time)
}

func NewSlogFormatter() *SlogFormatter {
	b := NewBuffer()
	f := &SlogFormatter{
		b:        b,
		stdLog:   log.New(b, "", log.LstdFlags),
		minLevel: new(slog.LevelVar),
		logLevel: true,
		mu:       new(sync.Mutex),
	}
	return f
}

type SlogFormatter struct {
	b    *Buffer
	hide map[string]struct{}
	json bool

	addSource bool
	logLevel  bool
	logTime   bool

	h        slog.Handler
	stdLog   *log.Logger
	minLevel *slog.LevelVar

	mu *sync.Mutex
}

var _ Formatter = (*SlogFormatter)(nil)

func (self *SlogFormatter) WithLogMetadata(v bool) *SlogFormatter {
	self.WithLogLevel(v)
	self.WithLogTime(v)
	return self
}

func (self *SlogFormatter) WithHideFields(fields []string) *SlogFormatter {
	self.hide = make(map[string]struct{}, len(fields))
	for _, field := range fields {
		self.hide[field] = struct{}{}
	}
	return self
}

func (self *SlogFormatter) WithJsonHandler() *SlogFormatter {
	self.json = true
	self.h = slog.NewJSONHandler(self.b, &slog.HandlerOptions{
		AddSource:   self.addSource,
		Level:       self.minLevel,
		ReplaceAttr: self.replaceHiddenAttr,
	})
	return self
}

func (self *SlogFormatter) WithLevel(level slog.Level) *SlogFormatter {
	self.minLevel.Set(level)
	return self
}

func (self *SlogFormatter) WithLogTime(enable bool) *SlogFormatter {
	self.logTime = enable
	return self
}

func (self *SlogFormatter) WithLogLevel(enable bool) *SlogFormatter {
	self.logLevel = enable
	return self
}

func (self *SlogFormatter) WithTextHandler() *SlogFormatter {
	self.h = slog.NewTextHandler(self.b, &slog.HandlerOptions{
		AddSource:   self.addSource,
		Level:       self.minLevel,
		ReplaceAttr: self.replaceTextAttr,
	})
	return self
}

func (self *SlogFormatter) replaceTextAttr(groups []string, a slog.Attr,
) slog.Attr {
	switch a.Key {
	case slog.TimeKey, slog.LevelKey, slog.MessageKey:
		return slog.Attr{}
	}
	return self.replaceHiddenAttr(groups, a)
}

func (self *SlogFormatter) replaceHiddenAttr(_ []string, a slog.Attr) slog.Attr {
	if self.hiddenField(a.Key) {
		return slog.Attr{}
	}
	return a
}

func (self *SlogFormatter) hiddenField(name string) bool {
	_, hide := self.hide[name]
	return hide
}

func (self *SlogFormatter) FormatWithCallback(r slog.Record,
	cb func(b []byte) error,
) error {
	self.lock()
	defer self.unlock()
	if err := self.format(r); err != nil {
		return err
	}
	return cb(self.b.Bytes())
}

func (self *SlogFormatter) lock() {
	self.mu.Lock()
	self.b.Alloc()
}

func (self *SlogFormatter) unlock() {
	self.b.Free()
	self.mu.Unlock()
}

func (self *SlogFormatter) format(r slog.Record) error {
	if err := self.formatStd(r); err != nil {
		return nil
	}

	ctx := context.Background()
	if err := self.h.Handle(ctx, r); err != nil {
		return fmt.Errorf("failed slog handler: %w", err)
	}

	// Discard trailing '\n', added by slog.TextHandler, and trailing ' ' added by
	// formatStd.
	b := self.b.Bytes()
	b = bytes.TrimRightFunc(b, unicode.IsSpace)
	self.b.Truncate(len(b))
	return nil
}

func (self *SlogFormatter) formatStd(r slog.Record) error {
	if self.json {
		return nil
	}

	if self.logTime {
		// output log.LstdFlags
		if err := self.stdLog.Output(2, ""); err != nil {
			return fmt.Errorf("write prefix to log.Output: %w", err)
		}
		// Discard last byte (\n), added by log.Output.
		self.b.Truncate(self.b.Len() - 1)
	}

	if self.logLevel {
		self.b.WriteString(r.Level.String())
		self.b.WriteByte(' ')
	}

	self.b.WriteString(r.Message)
	self.b.WriteByte(' ')
	return nil
}

func (self *SlogFormatter) Write(w io.Writer, r slog.Record) error {
	self.lock()
	defer self.unlock()
	if err := self.format(r); err != nil {
		return err
	}
	self.b.WriteByte('\n')
	if _, err := self.b.WriteTo(w); err != nil {
		return fmt.Errorf("write formatted entry: %w", err)
	}
	return nil
}

func (self *SlogFormatter) Enabled(ctx context.Context, level slog.Level) bool {
	return self.h.Enabled(ctx, level)
}

func (self *SlogFormatter) WithAttrs(attrs []slog.Attr) Formatter {
	f := *self
	f.h = self.h.WithAttrs(attrs)
	return &f
}

func (self *SlogFormatter) WithGroup(name string) Formatter {
	f := *self
	f.h = self.h.WithGroup(name)
	return &f
}
