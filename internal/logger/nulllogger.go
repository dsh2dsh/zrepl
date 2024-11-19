package logger

type nullLogger struct{}

var _ Logger = (*nullLogger)(nil)

func NewNullLogger() Logger { return nullLogger{} }

func (n nullLogger) WithField(field string, val any) Logger { return n }
func (n nullLogger) WithError(err error) Logger             { return n }
func (nullLogger) Log(level Level, msg string)              {}
func (nullLogger) Debug(msg string)                         {}
func (nullLogger) Info(msg string)                          {}
func (nullLogger) Warn(msg string)                          {}
func (nullLogger) Error(msg string)                         {}
