package logging

import (
	"bytes"
	"fmt"
	"io"

	"github.com/go-logfmt/logfmt"

	"github.com/dsh2dsh/zrepl/logger"
)

const (
	FieldLevel   = "level"
	FieldMessage = "msg"
	FieldTime    = "time"
)

const (
	JobField    string = "job"
	SubsysField string = "subsystem"
	SpanField   string = "span"
)

type MetadataFlags int64

const (
	MetadataTime MetadataFlags = 1 << iota
	MetadataLevel
	MetadataColor

	MetadataNone MetadataFlags = 0
	MetadataAll  MetadataFlags = ^0
)

// --------------------------------------------------

type NoFormatter struct{}

func (f NoFormatter) SetMetadataFlags(flags MetadataFlags) {}

func (f NoFormatter) Write(w io.Writer, e *logger.Entry) error {
	return writeFormatEntry(w, e, f)
}

func writeFormatEntry(w io.Writer, e *logger.Entry, f EntryFormatter) error {
	b, err := f.Format(e)
	if err != nil {
		return err
	}
	if _, err := w.Write(b); err != nil {
		return fmt.Errorf("write formatted entry: %w", err)
	}
	return nil
}

func (f NoFormatter) Format(e *logger.Entry) ([]byte, error) {
	return []byte(e.Message), nil
}

// --------------------------------------------------

type LogfmtFormatter struct {
	metadataFlags MetadataFlags
}

func (f *LogfmtFormatter) SetMetadataFlags(flags MetadataFlags) {
	f.metadataFlags = flags
}

func (f *LogfmtFormatter) Format(e *logger.Entry) ([]byte, error) {
	var buf bytes.Buffer
	enc := logfmt.NewEncoder(&buf)

	if f.metadataFlags&MetadataTime != 0 {
		err := enc.EncodeKeyval(FieldTime, e.Time)
		if err != nil {
			return nil, fmt.Errorf("logfmt: encode time: %w", err)
		}
	}
	if f.metadataFlags&MetadataLevel != 0 {
		err := enc.EncodeKeyval(FieldLevel, e.Level)
		if err != nil {
			return nil, fmt.Errorf("logfmt: encode level: %w", err)
		}
	}

	// at least try and put job and task in front
	prefixed := make(map[string]bool, 3)
	prefix := []string{JobField, SubsysField, SpanField}
	for _, pf := range prefix {
		v, ok := e.Fields[pf]
		if !ok {
			break
		}
		if err := logfmtTryEncodeKeyval(enc, pf, v); err != nil {
			return nil, err // unlikely
		}
		prefixed[pf] = true
	}

	err := enc.EncodeKeyval(FieldMessage, e.Message)
	if err != nil {
		return nil, fmt.Errorf("logfmt: encode message: %w", err)
	}
	for k, v := range e.Fields {
		if !prefixed[k] {
			if err := logfmtTryEncodeKeyval(enc, k, v); err != nil {
				return nil, err
			}
		}
	}

	return buf.Bytes(), nil
}

func logfmtTryEncodeKeyval(enc *logfmt.Encoder, field, value interface{}) error {
	err := enc.EncodeKeyval(field, value)
	switch err {
	case nil: // ok
		return nil
	case logfmt.ErrUnsupportedValueType:
		err := enc.EncodeKeyval(field, fmt.Sprintf("<%T>", value))
		if err != nil {
			return fmt.Errorf("cannot encode unsupported value type Go type: %w", err)
		}
		return nil
	}
	return fmt.Errorf("cannot encode field '%s': %w", field, err)
}

func (f *LogfmtFormatter) Write(w io.Writer, e *logger.Entry) error {
	return writeFormatEntry(w, e, f)
}
