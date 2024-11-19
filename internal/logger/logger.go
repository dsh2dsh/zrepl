package logger

import (
	"fmt"
	"maps"
	"runtime/debug"
	"sync"
	"time"
)

const (
	DefaultUserFieldCapacity = 5
	// The field set by WithError function
	FieldError = "err"
)

type Logger interface {
	WithField(field string, val any) Logger
	WithError(err error) Logger
	Log(level Level, msg string)
	Debug(msg string)
	Info(msg string)
	Warn(msg string)
	Error(msg string)
}

type loggerImpl struct {
	fields        Fields
	outlets       *Outlets
	outletTimeout time.Duration

	mtx *sync.Mutex
}

var _ Logger = (*loggerImpl)(nil)

func NewLogger(outlets *Outlets, outletTimeout time.Duration) Logger {
	return &loggerImpl{
		fields:  make(Fields, DefaultUserFieldCapacity),
		outlets: outlets,
		mtx:     new(sync.Mutex),

		outletTimeout: outletTimeout,
	}
}

func (l *loggerImpl) logInternalError(outlet Outlet, err string) {
	fields := Fields{}
	if outlet != nil {
		if _, ok := outlet.(fmt.Stringer); ok {
			fields["outlet"] = fmt.Sprintf("%s", outlet)
		}
		fields["outlet_type"] = fmt.Sprintf("%T", outlet)
	}
	fields[FieldError] = err
	entry := Entry{
		Level:   Error,
		Message: "outlet error",
		Time:    time.Now(),
		Fields:  fields,
	}
	// ignore errors at this point (still better than panicking if the error is
	// temporary)
	_ = l.outlets.GetLoggerErrorOutlet().WriteEntry(entry)
}

func (l *loggerImpl) log(level Level, msg string) {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	entry := Entry{
		Level:   level,
		Message: msg,
		Time:    time.Now(),
		Fields:  l.fields,
	}

	louts := l.outlets.Get(level)
	for i := range louts {
		if err := louts[i].WriteEntry(entry); err != nil {
			l.logInternalError(louts[i], err.Error())
		}
	}
}

// callers must hold l.mtx
func (l *loggerImpl) forkLogger(field string, val any) *loggerImpl {
	child := &loggerImpl{
		fields:  make(Fields, len(l.fields)+1),
		outlets: l.outlets,
		mtx:     l.mtx,

		outletTimeout: l.outletTimeout,
	}
	maps.Copy(child.fields, l.fields)
	child.fields[field] = val
	return child
}

func (l *loggerImpl) WithField(field string, val any) Logger {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	if val, ok := l.fields[field]; ok && val != nil {
		l.logInternalError(nil, fmt.Sprintf(
			"caller overwrites field '%s'. Stack: %s", field, string(debug.Stack())))
	}
	return l.forkLogger(field, val)
}

func (l *loggerImpl) WithError(err error) Logger {
	if err != nil {
		return l.WithField(FieldError, err.Error())
	}
	return l
}

func (l *loggerImpl) Log(level Level, msg string) { l.log(level, msg) }

func (l *loggerImpl) Debug(msg string) { l.log(Debug, msg) }

func (l *loggerImpl) Info(msg string) { l.log(Info, msg) }

func (l *loggerImpl) Warn(msg string) { l.log(Warn, msg) }

func (l *loggerImpl) Error(msg string) { l.log(Error, msg) }
