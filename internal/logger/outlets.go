package logger

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

func NewOutlets(handlers ...slog.Handler) *Outlets {
	self := &Outlets{handlers: make([]slog.Handler, 0, max(2, len(handlers)))}
	return self.Add(handlers...)
}

type Outlets struct {
	*slog.MultiHandler

	handlers []slog.Handler
}

var _ slog.Handler = (*Outlets)(nil)

func (self *Outlets) Add(handlers ...slog.Handler) *Outlets {
	if len(handlers) == 0 {
		return self
	}

	self.handlers = append(self.handlers, handlers...)
	self.MultiHandler = slog.NewMultiHandler(self.handlers...)
	return self
}

func (self *Outlets) Handle(ctx context.Context, r slog.Record) error {
	err := self.MultiHandler.Handle(ctx, r)
	if err == nil {
		return nil
	}

	err = fmt.Errorf("logger: one of handlers failed: %w", err)
	self.logInternalError(ctx, err)
	return err
}

func (self *Outlets) logInternalError(ctx context.Context, err error) {
	o := self.errorOutlet(ctx)
	if o == nil {
		return
	}

	r := slog.NewRecord(time.Now(), slog.LevelError, "unable log message",
		uintptr(0))
	r.AddAttrs(slog.Any(fieldError, err))

	// ignore errors at this point (still better than panicking if the error is
	// temporary)
	_ = o.Handle(ctx, r)
}

// Return the first outlet added to this Outlets list using Add() with minLevel
// <= Error. If no such outlet is in this Outlets list, a discarding outlet is
// returned.
func (self *Outlets) errorOutlet(ctx context.Context) slog.Handler {
	for _, o := range self.handlers {
		if o.Enabled(ctx, slog.LevelError) {
			return o
		}
	}
	return nil
}
