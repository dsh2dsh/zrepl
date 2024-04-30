package job

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/robfig/cron/v3"

	"github.com/zrepl/zrepl/daemon/logging"
	"github.com/zrepl/zrepl/endpoint"
	"github.com/zrepl/zrepl/logger"
	"github.com/zrepl/zrepl/zfs"
)

type Logger = logger.Logger

func GetLogger(ctx context.Context) Logger {
	return logging.GetLogger(ctx, logging.SubsysJob)
}

type Job interface {
	Name() string
	Run(ctx context.Context, cron *cron.Cron)
	Status() *Status
	RegisterMetrics(registerer prometheus.Registerer)
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
	JobSpecific interface{}
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
		return fmt.Errorf("field 'type' not found")
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
	if v, ok := s.JobSpecific.(interface{ Error() string }); ok {
		return v.Error()
	}
	return ""
}

func (s *Status) Running() time.Duration {
	if v, ok := s.JobSpecific.(interface{ Running() time.Duration }); ok {
		return v.Running()
	}
	return 0
}
