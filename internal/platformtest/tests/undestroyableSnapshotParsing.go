package tests

import (
	"fmt"

	"github.com/stretchr/testify/require"

	"github.com/dsh2dsh/zrepl/internal/platformtest"
	"github.com/dsh2dsh/zrepl/internal/zfs"
)

func UndestroyableSnapshotParsing(t *platformtest.Context) {
	platformtest.Run(t, platformtest.PanicErr, t.RootDataset, `
		DESTROYROOT
		CREATEROOT
		+  "foo bar"
		+  "foo bar@1 2 3"
		+  "foo bar@4 5 6"
		+  "foo bar@7 8 9"
		R  zfs hold zrepl_platformtest "${ROOTDS}/foo bar@4 5 6"
	`)

	err := zfs.ZFSDestroy(t, t.RootDataset+"/foo bar@1 2 3,4 5 6,7 8 9")
	if err == nil {
		panic("expecting destroy error due to hold")
	}
	if dse, ok := err.(*zfs.DestroySnapshotsError); !ok {
		panic(fmt.Sprintf("expecting *zfs.DestroySnapshotsError, got %T\n%v\n%s", err, err, err))
	} else {
		if dse.Filesystem != t.RootDataset+"/foo bar" {
			panic(dse.Filesystem)
		}
		require.Equal(t, []string{"4 5 6"}, dse.Undestroyable)
		require.Equal(t, []string{"dataset is busy"}, dse.Reason)
	}
}
