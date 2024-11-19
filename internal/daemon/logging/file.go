package logging

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"syscall"

	"github.com/dsh2dsh/zrepl/internal/config"
)

func parseFileOutlet(in *config.FileLoggingOutlet, formatter *SlogFormatter,
) (*FileOutlet, error) {
	return NewFileOutlet(in.FileName, formatter)
}

func NewFileOutlet(filename string, formatter *SlogFormatter,
) (*FileOutlet, error) {
	outlet := new(FileOutlet).WithFormatter(formatter)
	if filename == "" {
		return outlet.WithWriter(log.Default().Writer()), nil
	}

	f, err := NewLogFile(filename)
	if err != nil {
		return nil, err
	}
	return outlet.WithWriter(f), nil
}

type FileOutlet struct {
	formatter Formatter
	w         io.Writer
}

var _ slog.Handler = (*FileOutlet)(nil)

func (self *FileOutlet) WithFormatter(f Formatter) *FileOutlet {
	self.formatter = f
	return self
}

func (self *FileOutlet) WithWriter(w io.Writer) *FileOutlet {
	self.w = w
	return self
}

func (self *FileOutlet) Enabled(ctx context.Context, level slog.Level) bool {
	return self.formatter.Enabled(ctx, level)
}

func (self *FileOutlet) Handle(_ context.Context, r slog.Record) error {
	if err := self.formatter.Write(self.w, r); err != nil {
		return fmt.Errorf("write log entry: %w", err)
	}
	return nil
}

func (self *FileOutlet) WithAttrs(attrs []slog.Attr) slog.Handler {
	o := *self
	o.formatter = self.formatter.WithAttrs(attrs)
	return &o
}

func (self *FileOutlet) WithGroup(name string) slog.Handler {
	o := *self
	o.formatter = self.formatter.WithGroup(name)
	return &o
}

// --------------------------------------------------

func NewLogFile(filename string) (f *logFile, err error) {
	f = &logFile{filename: filename}
	if err := f.Open(); err != nil {
		return nil, err
	}
	return f, nil
}

type logFile struct {
	f        *os.File
	filename string
}

func (self *logFile) Write(p []byte) (int, error) {
	if err := self.reopenIfNotExists(); err != nil {
		return 0, fmt.Errorf("reopen file %q: %w", self.filename, err)
	}
	n, err := self.f.Write(p)
	if err != nil {
		return n, fmt.Errorf("write to %q: %w", self.filename, err)
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
	finfo, err := self.f.Stat()
	if err != nil {
		return false, fmt.Errorf("stat of %q: %w", self.filename, err)
	}

	if finfo.Sys() != nil {
		if stat, ok := finfo.Sys().(*syscall.Stat_t); ok {
			return stat.Nlink > 0, nil
		}
	}
	return false, nil
}

func (self *logFile) reopen() error {
	if err := self.f.Close(); err != nil {
		return fmt.Errorf("close %q: %w", self.filename, err)
	}
	return self.Open()
}

func (self *logFile) Open() error {
	f, err := os.OpenFile(self.filename,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	self.f = f
	return nil
}
