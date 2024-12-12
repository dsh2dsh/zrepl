package logger

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"
)

func NewOutlets() *Outlets {
	return &Outlets{outs: make([]slog.Handler, 0, 2)}
}

type Outlets struct {
	outs []slog.Handler
}

var _ slog.Handler = (*Outlets)(nil)

func (self *Outlets) Add(outlet slog.Handler) *Outlets {
	self.outs = append(self.outs, outlet)
	return self
}

func (self *Outlets) Enabled(ctx context.Context, level slog.Level) bool {
	for _, o := range self.outs {
		if o.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (self *Outlets) Handle(ctx context.Context, r slog.Record) error {
	for i, o := range self.outs {
		if !o.Enabled(ctx, r.Level) {
			continue
		}
		if err := o.Handle(ctx, r.Clone()); err != nil {
			self.logInternalError(o, err.Error())
			return fmt.Errorf("outlet #%v: %w", i, err)
		}
	}
	return nil
}

func (self *Outlets) logInternalError(outlet slog.Handler, err string) {
	r := slog.NewRecord(time.Now(), slog.LevelError, "outlet error", uintptr(0))
	if outlet != nil {
		if _, ok := outlet.(fmt.Stringer); ok {
			r.AddAttrs(slog.String("outlet", fmt.Sprintf("%s", outlet)))
		}
		r.AddAttrs(slog.String("outlet_type", fmt.Sprintf("%T", outlet)))
	}
	r.AddAttrs(slog.String(fieldError, err))
	if o := self.GetLoggerErrorOutlet(); o != nil {
		// ignore errors at this point (still better than panicking if the error is
		// temporary)
		_ = o.Handle(context.Background(), r)
	}
}

// Return the first outlet added to this Outlets list using Add() with minLevel
// <= Error. If no such outlet is in this Outlets list, a discarding outlet is
// returned.
func (self *Outlets) GetLoggerErrorOutlet() slog.Handler {
	for _, o := range self.outs {
		if o.Enabled(context.Background(), slog.LevelError) {
			return o
		}
	}
	return nil
}

func (self *Outlets) WithAttrs(attrs []slog.Attr) slog.Handler {
	outs := slices.Clone(self.outs)
	for i := range self.outs {
		outs[i] = self.outs[i].WithAttrs(attrs)
	}
	return &Outlets{outs: outs}
}

func (self *Outlets) WithGroup(name string) slog.Handler {
	outs := make([]slog.Handler, len(self.outs))
	for i := range self.outs {
		outs[i] = self.outs[i].WithGroup(name)
	}
	return &Outlets{outs: outs}
}
