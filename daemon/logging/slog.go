package logging

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"slices"
	"sync"

	"github.com/dsh2dsh/zrepl/config"
	"github.com/dsh2dsh/zrepl/logger"
)

func parseSlogFormatter(in *config.LoggingOutletCommon) *SlogFormatter {
	return NewSlogFormatter().WithHideFields(in.HideFields).WithLogTime(in.Time)
}

func NewSlogFormatter() *SlogFormatter {
	var b bytes.Buffer
	f := &SlogFormatter{
		b:         &b,
		log:       log.New(&b, "", log.LstdFlags),
		maxLevel:  new(slog.LevelVar),
		metaFlags: MetadataLevel,
		mu:        new(sync.Mutex),
	}
	orderedFields := [...]string{SubsysField, JobField, SpanField}
	return f.withOrderedFields(orderedFields[:])
}

type SlogFormatter struct {
	b         *bytes.Buffer
	hide      map[string]struct{}
	json      bool
	logTime   bool
	metaFlags MetadataFlags

	ordered     []string
	skipOrdered map[string]struct{}

	log      *log.Logger
	logger   *slog.Logger
	maxLevel *slog.LevelVar

	mu *sync.Mutex
}

func (self *SlogFormatter) withOrderedFields(fields []string) *SlogFormatter {
	self.ordered = fields
	self.skipOrdered = make(map[string]struct{}, len(fields))
	for _, k := range fields {
		self.skipOrdered[k] = struct{}{}
	}
	return self
}

func (self *SlogFormatter) SetMetadataFlags(f MetadataFlags) {
	self.metaFlags = f
	self.WithLogTime(f&MetadataTime == 1)
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
	self.logger = slog.New(slog.NewJSONHandler(self.b, &slog.HandlerOptions{
		Level:       self.maxLevel,
		ReplaceAttr: self.replaceAttr,
	}))
	return self
}

func (self *SlogFormatter) WithLevel(l logger.Level) *SlogFormatter {
	self.maxLevel.Set(self.level(l))
	return self
}

func (self *SlogFormatter) level(l logger.Level) slog.Level {
	switch l {
	case logger.Debug:
		return slog.LevelDebug
	case logger.Info:
		return slog.LevelInfo
	case logger.Warn:
		return slog.LevelWarn
	}
	return slog.LevelError
}

func (self *SlogFormatter) WithLogTime(enable bool) *SlogFormatter {
	self.logTime = enable
	return self
}

func (self *SlogFormatter) WithTextHandler() *SlogFormatter {
	self.logger = slog.New(slog.NewTextHandler(self.b, &slog.HandlerOptions{
		Level:       self.maxLevel,
		ReplaceAttr: self.replaceAttr,
	}))
	return self
}

func (self *SlogFormatter) replaceAttr(groups []string, a slog.Attr) slog.Attr {
	if !self.json {
		if a.Key == slog.TimeKey || a.Key == slog.LevelKey || a.Key == slog.MessageKey {
			return slog.Attr{}
		}
	}
	if self.hiddenField(a.Key) {
		return slog.Attr{}
	}
	return a
}

func (self *SlogFormatter) hiddenField(name string) bool {
	_, hide := self.hide[name]
	return hide
}

func (self *SlogFormatter) Format(e *logger.Entry) ([]byte, error) {
	self.mu.Lock()
	defer func() {
		self.b.Reset()
		self.mu.Unlock()
	}()

	if err := self.format(e); err != nil {
		return nil, err
	}
	return bytes.Clone(self.b.Bytes()), nil
}

func (self *SlogFormatter) Write(w io.Writer, e *logger.Entry) error {
	self.mu.Lock()
	defer func() {
		self.b.Reset()
		self.mu.Unlock()
	}()

	if err := self.format(e); err != nil {
		return err
	}
	self.b.WriteByte('\n')
	if _, err := w.Write(self.b.Bytes()); err != nil {
		return fmt.Errorf("write formatted entry: %w", err)
	}
	return nil
}

func (self *SlogFormatter) format(e *logger.Entry) error {
	if !self.json {
		if self.logTime {
			// output log.LstdFlags
			if err := self.log.Output(2, ""); err != nil {
				return fmt.Errorf("write prefix to log.Output: %w", err)
			}
			// Discard last byte (\n), added by log.Output.
			self.b.Truncate(self.b.Len() - 1)
		}

		if self.metaFlags&MetadataLevel != 0 {
			self.b.WriteString(self.level(e.Level).String())
			self.b.WriteByte(' ')
		}

		self.b.WriteString(e.Message)
		self.b.WriteByte(' ')
	}

	self.logger.LogAttrs(context.Background(), self.level(e.Level), e.Message,
		self.attrs(e)...)
	// Discard last byte (\n), added by LogAttrs.
	self.b.Truncate(self.b.Len() - 1)
	return nil
}

func (self *SlogFormatter) attrs(e *logger.Entry) []slog.Attr {
	attrs := make([]slog.Attr, 0, len(e.Fields))
	for _, k := range self.ordered {
		if !self.hiddenField(k) {
			if v, ok := e.Fields[k]; ok {
				attrs = append(attrs, slog.Any(k, v))
			}
		}
	}

	orderedLen := len(attrs)
	for k, v := range e.Fields {
		if !self.orderedField(k) && !self.hiddenField(k) {
			attrs = append(attrs, slog.Any(k, v))
		}
	}

	if len(attrs) > orderedLen {
		slices.SortFunc(attrs[orderedLen:], func(a, b slog.Attr) int {
			return cmp.Compare(a.Key, b.Key)
		})
	}
	return attrs
}

func (self *SlogFormatter) orderedField(name string) bool {
	_, ok := self.skipOrdered[name]
	return ok
}
