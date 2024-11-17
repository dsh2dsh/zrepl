package pruner

import (
	"time"

	"github.com/dsh2dsh/zrepl/internal/pruning"
	"github.com/dsh2dsh/zrepl/internal/replication/logic/pdu"
)

type snapshot struct {
	replicated bool
	date       time.Time
	fsv        *pdu.FilesystemVersion
}

var _ pruning.Snapshot = (*snapshot)(nil)

func (s snapshot) Report() SnapshotReport {
	return SnapshotReport{
		Name:       s.Name(),
		Replicated: s.Replicated(),
		Date:       s.Date(),
	}
}

func (s *snapshot) Name() string { return s.fsv.Name }

func (s *snapshot) Replicated() bool { return s.replicated }

func (s *snapshot) Date() time.Time { return s.date }
