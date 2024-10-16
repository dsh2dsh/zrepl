package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/dsh2dsh/cron/v3"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/dsh2dsh/zrepl/internal/daemon/logging"
	"github.com/dsh2dsh/zrepl/internal/endpoint"
	"github.com/dsh2dsh/zrepl/internal/logger"
	"github.com/dsh2dsh/zrepl/internal/zfs"
)

type Logger = logger.Logger

func GetLogger(ctx context.Context) Logger {
	return logging.GetLogger(ctx, logging.SubsysJob)
}

type Internal interface {
	Run(ctx context.Context, cron *cron.Cron) error
	RegisterMetrics(registerer prometheus.Registerer)
	Shutdown()
}

type Job interface {
	Internal

	Name() string
	Status() *Status
	// Jobs that return a subtree of the dataset hierarchy
	// must return the root of that subtree as rfs and ok = true
	OwnedDatasetSubtreeRoot() (rfs *zfs.DatasetPath, ok bool)
	SenderConfig() *endpoint.SenderConfig
}

type Type string

const (
	TypeInternal Type = "internal"
	TypeSnap     Type = "snap"
	TypePush     Type = "push"
	TypeSink     Type = "sink"
	TypePull     Type = "pull"
	TypeSource   Type = "source"
)

type Status struct {
	Type        Type
	JobSpecific JobStatus
}

type JobStatus interface {
	Error() string
	Running() (time.Duration, bool)
	Cron() string
	SleepingUntil() time.Time
	Steps() (expected, step int)
	Progress() (expected, completed uint64)
}

func (s *Status) MarshalJSON() ([]byte, error) {
	typeJson, err := json.Marshal(s.Type)
	if err != nil {
		return nil, err
	}
	jobJSON, err := json.Marshal(s.JobSpecific)
	if err != nil {
		return nil, err
	}
	m := map[string]json.RawMessage{
		"type":         typeJson,
		string(s.Type): jobJSON,
	}
	return json.Marshal(m)
}

func (s *Status) UnmarshalJSON(in []byte) (err error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(in, &m); err != nil {
		return err
	}
	tJSON, ok := m["type"]
	if !ok {
		return errors.New("field 'type' not found")
	}
	if err := json.Unmarshal(tJSON, &s.Type); err != nil {
		return err
	}
	key := string(s.Type)
	jobJSON, ok := m[key]
	if !ok {
		return fmt.Errorf("field '%s', not found", key)
	}
	switch s.Type {
	case TypeSnap:
		var st SnapJobStatus
		err = json.Unmarshal(jobJSON, &st)
		s.JobSpecific = &st

	case TypePull:
		fallthrough
	case TypePush:
		var st ActiveSideStatus
		err = json.Unmarshal(jobJSON, &st)
		s.JobSpecific = &st

	case TypeSource:
		fallthrough
	case TypeSink:
		var st PassiveStatus
		err = json.Unmarshal(jobJSON, &st)
		s.JobSpecific = &st

	case TypeInternal:
		// internal jobs do not report specifics
	default:
		err = fmt.Errorf("unknown job type '%s'", key)
	}
	return err
}

func (s *Status) Error() string {
	return s.JobSpecific.Error()
}

func (s *Status) Running() (time.Duration, bool) {
	return s.JobSpecific.Running()
}

func (s *Status) Internal() bool { return s.Type == TypeInternal }

func (s *Status) Cron() string {
	return s.JobSpecific.Cron()
}

func (s *Status) SleepingUntil() time.Time {
	return s.JobSpecific.SleepingUntil()
}

func (s *Status) CanSignal() string {
	if _, ok := s.Running(); ok {
		return "reset"
	} else if t := s.SleepingUntil(); !t.IsZero() {
		return "wakeup"
	}
	return ""
}

func (s *Status) Steps() (expected, step int) {
	return s.JobSpecific.Steps()
}

func (s *Status) Progress() (uint64, uint64) {
	return s.JobSpecific.Progress()
}
