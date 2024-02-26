package logging

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"slices"
	"sync"
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
	outlet.WithHideFields(in.HideFields).WithLevel(level).WithLogTime(in.Time)
	return outlet, nil
}

func newFileOutlet(filename string) (*FileOutlet, error) {
	orderedFields := [...]string{JobField, SubsysField, SpanField}
	outlet := new(FileOutlet).withOrderedFields(orderedFields[:])

	var w io.Writer = log.Default().Writer()
	if filename != "" {
		f, err := newLogFile(filename)
		if err != nil {
			return nil, err
		}
		f.WithErrorHandler(outlet.writeError)
		w = f
	}

	return outlet.WithTextHandler(log.New(w, "", log.LstdFlags)), nil
}

type FileOutlet struct {
	hide map[string]struct{}

	log      *log.Logger
	logger   *slog.Logger
	maxLevel *slog.LevelVar

	ordered     []string
	skipOrdered map[string]struct{}

	lastErr error
}

func (self *FileOutlet) withOrderedFields(fields []string) *FileOutlet {
	self.ordered = fields
	self.skipOrdered = make(map[string]struct{}, len(fields))
	for _, k := range fields {
		self.skipOrdered[k] = struct{}{}
	}
	return self
}

func (self *FileOutlet) WithHideFields(fields []string) *FileOutlet {
	self.hide = make(map[string]struct{}, len(fields))
	for _, field := range fields {
		self.hide[field] = struct{}{}
	}
	return self
}

func (self *FileOutlet) WithLevel(l logger.Level) *FileOutlet {
	self.maxLevel.Set(self.level(l))
	return self
}

func (self *FileOutlet) WithLogTime(enable bool) *FileOutlet {
	flags := self.log.Flags()
	if enable {
		flags |= log.LstdFlags
	} else {
		flags &= ^log.LstdFlags
	}
	self.log.SetFlags(flags)
	return self
}

func (self *FileOutlet) WithTextHandler(log *log.Logger) *FileOutlet {
	self.log = log
	self.maxLevel = new(slog.LevelVar)

	self.logger = slog.New(newTextHandler(self.log, &slog.HandlerOptions{
		Level:       self.maxLevel,
		ReplaceAttr: self.replaceAttr,
	}))
	return self
}

func (self *FileOutlet) replaceAttr(groups []string, a slog.Attr) slog.Attr {
	if self.hiddenField(a.Key) {
		return slog.Attr{}
	}
	return a
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
	self.logger.LogAttrs(context.Background(), self.level(e.Level), e.Message,
		attrs...)
	if err := self.lastErr; err != nil {
		self.lastErr = nil
		return fmt.Errorf("failed log entry: %w", err)
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

// --------------------------------------------------

func newTextHandler(log *log.Logger, opts *slog.HandlerOptions) *TextHandler {
	var b bytes.Buffer
	if opts == nil {
		opts = &slog.HandlerOptions{}
	}
	h := TextHandler{
		log: log,
		b:   &b,
		mu:  new(sync.Mutex),

		nextReplace: opts.ReplaceAttr,
	}

	opts.ReplaceAttr = h.replaceAttr
	h.h = slog.NewTextHandler(&b, opts)
	return &h
}

type TextHandler struct {
	log *log.Logger
	h   slog.Handler
	b   *bytes.Buffer
	mu  *sync.Mutex

	nextReplace func(groups []string, a slog.Attr) slog.Attr
}

func (self *TextHandler) replaceAttr(groups []string, a slog.Attr) slog.Attr {
	if a.Key == slog.TimeKey || a.Key == slog.LevelKey || a.Key == slog.MessageKey {
		return slog.Attr{}
	}
	if self.nextReplace != nil {
		return self.nextReplace(groups, a)
	}
	return a
}

func (self *TextHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return self.h.Enabled(ctx, l)
}

func (self *TextHandler) Handle(ctx context.Context, r slog.Record) error {
	self.mu.Lock()
	defer func() {
		self.b.Reset()
		self.mu.Unlock()
	}()

	self.b.WriteString(r.Level.String())
	self.b.WriteByte(' ')
	self.b.WriteString(r.Message)
	self.b.WriteByte(' ')

	if err := self.h.Handle(ctx, r); err != nil {
		return fmt.Errorf("error from next handler: %w", err)
	}

	if err := self.log.Output(2, self.b.String()); err != nil {
		return fmt.Errorf("error from log.Output: %w", err)
	}
	return nil
}

func (self *TextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	h := *self
	h.h = self.h.WithAttrs(attrs)
	return &h
}

func (self *TextHandler) WithGroup(name string) slog.Handler {
	h := *self
	h.h = self.h.WithGroup(name)
	return &h
}
