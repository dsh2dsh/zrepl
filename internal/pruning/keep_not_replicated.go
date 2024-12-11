package pruning

import "context"

type KeepNotReplicated struct{}

var _ KeepRule = (*KeepNotReplicated)(nil)

func (*KeepNotReplicated) KeepRule(_ context.Context, snaps []Snapshot,
) (destroyList []Snapshot) {
	return filterSnapList(snaps, func(snapshot Snapshot) bool {
		return snapshot.Replicated()
	})
}

func NewKeepNotReplicated() *KeepNotReplicated {
	return &KeepNotReplicated{}
}
