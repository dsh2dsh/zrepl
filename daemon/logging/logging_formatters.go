package logging

import (
	"fmt"
	"io"

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
