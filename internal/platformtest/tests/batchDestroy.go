package tests

import (
	"fmt"
	"strings"

	"github.com/dsh2dsh/zrepl/internal/platformtest"
	"github.com/dsh2dsh/zrepl/internal/zfs"
)

func BatchDestroy(ctx *platformtest.Context) {
	platformtest.Run(ctx, platformtest.PanicErr, ctx.RootDataset, `
		DESTROYROOT
		CREATEROOT
		+  "foo bar"
		+  "foo bar@1"
		+  "foo bar@2"
		+  "foo bar@3"
		R  zfs hold zrepl_platformtest "${ROOTDS}/foo bar@2"
	`)

	reqs := []*zfs.DestroySnapOp{
		{
			ErrOut:     new(error),
			Filesystem: ctx.RootDataset + "/foo bar",
			Name:       "3",
		},
		{
			ErrOut:     new(error),
			Filesystem: ctx.RootDataset + "/foo bar",
			Name:       "2",
		},
	}
	zfs.ZFSDestroyFilesystemVersions(ctx, reqs)
	if *reqs[0].ErrOut != nil {
		panic("expecting no error")
	}
	err := (*reqs[1].ErrOut).Error()
	if !strings.Contains(err, ctx.RootDataset+"/foo bar@2") {
		panic(fmt.Sprintf("expecting error about being unable to destroy @2: %T\n%s", err, err))
	}

	platformtest.Run(ctx, platformtest.PanicErr, ctx.RootDataset, `
	!N "foo bar@3"
	!E "foo bar@1"
	!E "foo bar@2"
	R zfs release zrepl_platformtest "${ROOTDS}/foo bar@2"
	-  "foo bar@2"
	-  "foo bar@1"
	-  "foo bar"
	`)
}
