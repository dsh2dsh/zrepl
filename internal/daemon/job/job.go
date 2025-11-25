package job

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/dsh2dsh/zrepl/internal/daemon/logging"
	"github.com/dsh2dsh/zrepl/internal/endpoint"
	"github.com/dsh2dsh/zrepl/internal/zfs"
)

func GetLogger(ctx context.Context) *slog.Logger {
	return logging.GetLogger(ctx, logging.SubsysJob)
}

type Internal interface {
	Run(ctx context.Context) error
	RegisterMetrics(registerer prometheus.Registerer)
}

type Job interface {
	Internal

	Name() string
	Status() *Status
	// Jobs that return a subtree of the dataset hierarchy
	// must return the root of that subtree as rfs and ok = true
	OwnedDatasetSubtreeRoot() (rfs *zfs.DatasetPath, ok bool)
	SenderConfig() *endpoint.SenderConfig
	Runnable() bool
	Cron() string
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
	Err       string
	NextCron  time.Time
	CanWakeup bool

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

func (s *Status) UnmarshalJSON(b []byte) error {
	st := struct{ Type Type }{}
	if err := json.Unmarshal(b, &st); err != nil {
		return fmt.Errorf("unmarshal status field 'Type': %w", err)
	}

	switch st.Type {
	case TypeInternal:
		// internal jobs do not report specifics
	case TypeSnap:
		s.JobSpecific = new(SnapJobStatus)
	case TypePull, TypePush:
		s.JobSpecific = new(ActiveSideStatus)
	case TypeSource, TypeSink:
		s.JobSpecific = new(PassiveStatus)
	default:
		return fmt.Errorf("unknown status type: %s", st.Type)
	}

	type status Status
	if err := json.Unmarshal(b, (*status)(s)); err != nil {
		return fmt.Errorf("unmarshal status as %q type: %w", st.Type, err)
	}
	return nil
}

func (s *Status) Error() string {
	if s := s.JobSpecific.Error(); s != "" {
		return s
	}
	return s.Err
}

func (s *Status) Running() (time.Duration, bool) {
	return s.JobSpecific.Running()
}

func (s *Status) Internal() bool { return s.Type == TypeInternal }

func (s *Status) Cron() string { return s.JobSpecific.Cron() }

func (s *Status) SleepingUntil() time.Time {
	t := s.JobSpecific.SleepingUntil()
	switch {
	case s.NextCron.IsZero():
		return t
	case t.IsZero():
		return s.NextCron
	case t.Before(s.NextCron):
		return t
	}
	return s.NextCron
}

func (s *Status) CanSignal() string {
	if _, ok := s.Running(); ok {
		return "reset"
	}

	if t := s.SleepingUntil(); !t.IsZero() {
		return "wakeup"
	}

	if s.CanWakeup {
		return "wakeup"
	}
	return ""
}

func (s *Status) Steps() (expected, step int) { return s.JobSpecific.Steps() }

func (s *Status) Progress() (uint64, uint64) { return s.JobSpecific.Progress() }
