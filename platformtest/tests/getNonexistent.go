package tests

import (
	"github.com/dsh2dsh/zrepl/platformtest"
	"github.com/dsh2dsh/zrepl/zfs"
)

func GetNonexistent(ctx *platformtest.Context) {
	platformtest.Run(ctx, platformtest.PanicErr, ctx.RootDataset, `
		DESTROYROOT
		CREATEROOT
		+  "foo bar"
		+  "foo bar@1"
	`)

	// test raw
	_, err := zfs.ZFSGetRawAnySource(ctx, ctx.RootDataset+"/foo bar", []string{"name"})
	if err != nil {
		panic(err)
	}

	// test nonexistent filesystem
	nonexistent := ctx.RootDataset + "/nonexistent filesystem"
	props, err := zfs.ZFSGetRawAnySource(ctx, nonexistent, []string{"name"})
	if err == nil {
		panic(props)
	}
	dsne, ok := err.(*zfs.DatasetDoesNotExist)
	if !ok {
		panic(err)
	} else if dsne.Path != nonexistent {
		panic(err)
	}

	// test nonexistent snapshot
	nonexistent = ctx.RootDataset + "/foo bar@non existent"
	props, err = zfs.ZFSGetRawAnySource(ctx, nonexistent, []string{"name"})
	if err == nil {
		panic(props)
	}
	dsne, ok = err.(*zfs.DatasetDoesNotExist)
	if !ok {
		panic(err)
	} else if dsne.Path != nonexistent {
		panic(err)
	}

	// test nonexistent bookmark
	nonexistent = ctx.RootDataset + "/foo bar#non existent"
	props, err = zfs.ZFSGetRawAnySource(ctx, nonexistent, []string{"name"})
	if err == nil {
		panic(props)
	}
	dsne, ok = err.(*zfs.DatasetDoesNotExist)
	if !ok {
		panic(err)
	} else if dsne.Path != nonexistent {
		panic(err)
	}
}
