package logging

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"
)

func NewTCPOutlet(formatter Formatter, network, address string,
	tlsConfig *tls.Config, retryInterval time.Duration,
) *TCPOutlet {
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
		return conn, fmt.Errorf("daemon/logging: %w", err)
	}

	// allow one message in flight while previous is in io.Copy()
	entryChan := make(chan *bytes.Buffer, 1)
	o := &TCPOutlet{
		formatter: formatter,
		connect:   connect,
		entryChan: entryChan,
	}
	go o.outLoop(retryInterval)
	return o
}

type TCPOutlet struct {
	formatter Formatter
	// Specifies how much time must pass between a connection error and a
	// reconnection attempt. Log entries written to the outlet during this time
	// interval are silently dropped.
	connect   func(ctx context.Context) (net.Conn, error)
	entryChan chan *bytes.Buffer
}

var _ slog.Handler = (*TCPOutlet)(nil)

// FIXME: use this method
func (h *TCPOutlet) Close() { close(h.entryChan) }

func (h *TCPOutlet) outLoop(retryInterval time.Duration) {
	var retry time.Time
	var conn net.Conn
	for msg := range h.entryChan {
		var err error
		for conn == nil {
			time.Sleep(time.Until(retry))
			ctx, cancel := context.WithDeadline(context.TODO(),
				time.Now().Add(retryInterval))
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

func (self *TCPOutlet) Enabled(ctx context.Context, level slog.Level) bool {
	return self.formatter.Enabled(ctx, level)
}

func (self *TCPOutlet) Handle(_ context.Context, r slog.Record) error {
	buf := new(bytes.Buffer)
	if err := self.formatter.Write(buf, r); err != nil {
		return err
	}

	select {
	case self.entryChan <- buf:
		return nil
	default:
		return errors.New("connection broken or not fast enough")
	}
}

func (self *TCPOutlet) WithAttrs(attrs []slog.Attr) slog.Handler {
	o := *self
	o.formatter = self.formatter.WithAttrs(attrs)
	return &o
}

func (self *TCPOutlet) WithGroup(name string) slog.Handler {
	o := *self
	o.formatter = self.formatter.WithGroup(name)
	return &o
}
