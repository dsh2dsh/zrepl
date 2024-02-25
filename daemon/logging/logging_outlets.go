package logging

import (
	"bytes"
	"cmp"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"log/slog"
	"log/syslog"
	"net"
	"os"
	"slices"
	"syscall"
	"time"

	"github.com/pkg/errors"

	"github.com/zrepl/zrepl/logger"
)

type EntryFormatter interface {
	SetMetadataFlags(flags MetadataFlags)
	Format(e *logger.Entry) ([]byte, error)
}

type WriterOutlet struct {
	formatter EntryFormatter
	writer    io.Writer
}

func (h WriterOutlet) WriteEntry(entry logger.Entry) error {
	bytes, err := h.formatter.Format(&entry)
	if err != nil {
		return err
	}
	_, err = h.writer.Write(bytes)
	if err != nil {
		return err
	}
	_, err = h.writer.Write([]byte("\n"))
	return err
}

type TCPOutlet struct {
	formatter EntryFormatter
	// Specifies how much time must pass between a connection error and a reconnection attempt
	// Log entries written to the outlet during this time interval are silently dropped.
	connect   func(ctx context.Context) (net.Conn, error)
	entryChan chan *bytes.Buffer
}

func NewTCPOutlet(formatter EntryFormatter, network, address string, tlsConfig *tls.Config, retryInterval time.Duration) *TCPOutlet {

	connect := func(ctx context.Context) (conn net.Conn, err error) {
		deadl, ok := ctx.Deadline()
		if !ok {
			deadl = time.Time{}
		}
		dialer := net.Dialer{
			Deadline: deadl,
		}
		if tlsConfig != nil {
			conn, err = tls.DialWithDialer(&dialer, network, address, tlsConfig)
		} else {
			conn, err = dialer.DialContext(ctx, network, address)
		}
		return
	}

	entryChan := make(chan *bytes.Buffer, 1) // allow one message in flight while previous is in io.Copy()

	o := &TCPOutlet{
		formatter: formatter,
		connect:   connect,
		entryChan: entryChan,
	}

	go o.outLoop(retryInterval)

	return o
}

// FIXME: use this method
func (h *TCPOutlet) Close() {
	close(h.entryChan)
}

func (h *TCPOutlet) outLoop(retryInterval time.Duration) {

	var retry time.Time
	var conn net.Conn
	for msg := range h.entryChan {
		var err error
		for conn == nil {
			time.Sleep(time.Until(retry))
			ctx, cancel := context.WithDeadline(context.TODO(), time.Now().Add(retryInterval))
			conn, err = h.connect(ctx)
			cancel()
			if err != nil {
				retry = time.Now().Add(retryInterval)
				conn = nil
			}
		}
		err = conn.SetWriteDeadline(time.Now().Add(retryInterval))
		if err == nil {
			_, err = io.Copy(conn, msg)
		}
		if err != nil {
			retry = time.Now().Add(retryInterval)
			conn.Close()
			conn = nil
		}
	}
}

func (h *TCPOutlet) WriteEntry(e logger.Entry) error {

	ebytes, err := h.formatter.Format(&e)
	if err != nil {
		return err
	}

	buf := new(bytes.Buffer)
	buf.Write(ebytes)
	buf.WriteString("\n")

	select {
	case h.entryChan <- buf:
		return nil
	default:
		return errors.New("connection broken or not fast enough")
	}
}

type SyslogOutlet struct {
	Formatter          EntryFormatter
	RetryInterval      time.Duration
	Facility           syslog.Priority
	writer             *syslog.Writer
	lastConnectAttempt time.Time
}

func (o *SyslogOutlet) WriteEntry(entry logger.Entry) error {

	bytes, err := o.Formatter.Format(&entry)
	if err != nil {
		return err
	}

	s := string(bytes)

	if o.writer == nil {
		now := time.Now()
		if now.Sub(o.lastConnectAttempt) < o.RetryInterval {
			return nil // not an error toward logger
		}
		o.writer, err = syslog.New(o.Facility, "zrepl")
		o.lastConnectAttempt = time.Now()
		if err != nil {
			o.writer = nil
			return err
		}
	}

	switch entry.Level {
	case logger.Debug:
		return o.writer.Debug(s)
	case logger.Info:
		return o.writer.Info(s)
	case logger.Warn:
		return o.writer.Warning(s)
	case logger.Error:
		return o.writer.Err(s)
	default:
		return o.writer.Err(s) // write as error as reaching this case is in fact an error
	}
}

// --------------------------------------------------

func newFileOutlet(filename string) (*FileOutlet, error) {
	if filename != "" {
		f, err := newLogFile(filename)
		if err != nil {
			return nil, err
		}
		f.WithErrorHandler(func(err error) error {
			log.SetOutput(os.Stderr)
			slog.LogAttrs(context.Background(), slog.LevelError,
				"error writing log message", slog.Any("error", err))
			return err
		})
		log.SetOutput(f)
	}
	orderedFields := [...]string{JobField, SubsysField, SpanField}
	return new(FileOutlet).withOrderedFields(orderedFields[:]), nil
}

type FileOutlet struct {
	hide map[string]struct{}

	ordered     []string
	skipOrdered map[string]struct{}
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

func (self *FileOutlet) WriteEntry(e logger.Entry) error {
	attrs := self.attrs(&e)
	slog.LogAttrs(context.Background(), self.level(e.Level), e.Message, attrs...)
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
	return n, self.handleError(err)
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
