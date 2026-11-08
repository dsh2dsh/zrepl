package pruner

import (
	"fmt"
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

func (self *snapshot) Report() SnapshotReport {
	return SnapshotReport{
		Name:       self.Name(),
		Replicated: self.Replicated(),
		Date:       self.Date(),
	}
}

func (self *snapshot) Name() string     { return self.fsv.Name }
func (self *snapshot) Replicated() bool { return self.replicated }
func (self *snapshot) Date() time.Time  { return self.date }

func (self *snapshot) Destroyable() error {
	if self.fsv.UserRefs == 0 && self.fsv.Clones == "" {
		return nil
	}
	return fmt.Errorf("something holds it: clones=%v userrefs=%v",
		self.fsv.Clones, self.fsv.UserRefs)
}
