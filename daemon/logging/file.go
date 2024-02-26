package logging

import (
	"cmp"
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"slices"
	"syscall"

	"github.com/zrepl/zrepl/config"
	"github.com/zrepl/zrepl/logger"
)

func parseFileOutlet(in *config.FileLoggingOutlet, level logger.Level,
) (*FileOutlet, error) {
	outlet, err := newFileOutlet(in.FileName)
	if err != nil {
		return nil, err
	}
	return outlet.WithHideFields(in.HideFields).WithLevel(level), nil
}

func newFileOutlet(filename string) (*FileOutlet, error) {
	orderedFields := [...]string{JobField, SubsysField, SpanField}
	outlet := new(FileOutlet).withOrderedFields(orderedFields[:])

	if filename != "" {
		f, err := newLogFile(filename)
		if err != nil {
			return nil, err
		}
		f.WithErrorHandler(outlet.writeError)
		log.SetOutput(f)
	}
	return outlet, nil
}

type FileOutlet struct {
	hide map[string]struct{}

	ordered     []string
	skipOrdered map[string]struct{}

	lastErr error
}

func (self *FileOutlet) WithHideFields(fields []string) *FileOutlet {
	self.hide = make(map[string]struct{}, len(fields))
	for _, field := range fields {
		self.hide[field] = struct{}{}
	}
	return self
}

func (self *FileOutlet) WithLevel(l logger.Level) *FileOutlet {
	slog.SetLogLoggerLevel(self.level(l))
	return self
}

func (self *FileOutlet) withOrderedFields(fields []string) *FileOutlet {
	self.ordered = fields
	self.skipOrdered = make(map[string]struct{}, len(fields))
	for _, k := range fields {
		self.skipOrdered[k] = struct{}{}
	}
	return self
}

func (self *FileOutlet) level(l logger.Level) slog.Level {
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

func (self *FileOutlet) writeError(err error) error {
	self.lastErr = err
	return err
}

func (self *FileOutlet) WriteEntry(e logger.Entry) error {
	attrs := self.attrs(&e)
	slog.LogAttrs(context.Background(), self.level(e.Level), e.Message, attrs...)
	if self.lastErr != nil {
		log.SetOutput(os.Stderr)
		slog.LogAttrs(context.Background(), slog.LevelError,
			"error writing log message", slog.Any("error", self.lastErr))
		self.lastErr = nil
		slog.LogAttrs(context.Background(), self.level(e.Level), e.Message, attrs...)
	}
	return nil
}

func (self *FileOutlet) attrs(e *logger.Entry) []slog.Attr {
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

func (self *FileOutlet) hiddenField(name string) bool {
	_, hide := self.hide[name]
	return hide
}

func (self *FileOutlet) orderedField(name string) bool {
	_, ok := self.skipOrdered[name]
	return ok
}

// --------------------------------------------------

func newLogFile(filename string) (f *logFile, err error) {
	f = &logFile{filename: filename}
	err = f.Open()
	return
}

type logFile struct {
	file         *os.File
	filename     string
	errorHandler func(err error) error
}

func (self *logFile) WithErrorHandler(fn func(err error) error) *logFile {
	self.errorHandler = fn
	return self
}

func (self *logFile) Write(p []byte) (int, error) {
	if err := self.reopenIfNotExists(); err != nil {
		return 0, self.handleError(err)
	}
	n, err := self.file.Write(p)
	if err != nil {
		return n, self.handleError(err)
	}
	return n, nil
}

func (self *logFile) reopenIfNotExists() error {
	if ok, err := self.exists(); err != nil {
		return err
	} else if ok {
		return nil
	}
	return self.reopen()
}

func (self *logFile) exists() (bool, error) {
	finfo, err := self.file.Stat()
	if err != nil {
		return false, fmt.Errorf("stat of %q: %w", self.filename, err)
	}

	nlink := uint64(0)
	if finfo.Sys() != nil {
		if stat, ok := finfo.Sys().(*syscall.Stat_t); ok {
			nlink = stat.Nlink
		}
	}
	return nlink > 0, nil
}

func (self *logFile) reopen() error {
	if err := self.file.Close(); err != nil {
		return fmt.Errorf("close %q: %w", self.filename, err)
	}
	return self.Open()
}

func (self *logFile) Open() error {
	f, err := os.OpenFile(self.filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	self.file = f
	return nil
}

func (self *logFile) handleError(err error) error {
	if self.errorHandler != nil {
		return self.errorHandler(err)
	}
	return err
}
