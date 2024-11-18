package logging

import (
	"fmt"
	"io"
	"log"
	"os"
	"syscall"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/logger"
)

func parseFileOutlet(in *config.FileLoggingOutlet, formatter EntryFormatter,
) (*FileOutlet, error) {
	return newFileOutlet(in.FileName, formatter)
}

func newFileOutlet(filename string, formatter EntryFormatter,
) (*FileOutlet, error) {
	outlet := new(FileOutlet).WithFormatter(formatter)
	if filename == "" {
		return outlet.WithWriter(log.Default().Writer()), nil
	}

	f, err := newLogFile(filename)
	if err != nil {
		return nil, err
	}
	return outlet.WithWriter(f), nil
}

type FileOutlet struct {
	formatter EntryFormatter
	w         io.Writer
}

func (self *FileOutlet) WithFormatter(f EntryFormatter) *FileOutlet {
	self.formatter = f
	return self
}

func (self *FileOutlet) WithWriter(w io.Writer) *FileOutlet {
	self.w = w
	return self
}

func (self *FileOutlet) WriteEntry(e logger.Entry) error {
	if err := self.formatter.Write(self.w, &e); err != nil {
		return fmt.Errorf("write log entry: %w", err)
	}
	return nil
}

// --------------------------------------------------

func newLogFile(filename string) (f *logFile, err error) {
	f = &logFile{filename: filename}
	err = f.Open()
	return
}

type logFile struct {
	file     *os.File
	filename string
}

func (self *logFile) Write(p []byte) (int, error) {
	if err := self.reopenIfNotExists(); err != nil {
		return 0, fmt.Errorf("reopen file %q: %w", self.filename, err)
	}
	n, err := self.file.Write(p)
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
	finfo, err := self.file.Stat()
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
	if err := self.file.Close(); err != nil {
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
	self.file = f
	return nil
}
