package tests

import (
	"fmt"
	"log"

	"github.com/dsh2dsh/zrepl/internal/platformtest"
	"github.com/dsh2dsh/zrepl/internal/zfs"
)

func IdempotentDestroy(ctx *platformtest.Context) {
	platformtest.Run(ctx, platformtest.PanicErr, ctx.RootDataset, `
		DESTROYROOT
		CREATEROOT
		+  "foo bar"
		+  "foo bar@a snap"
	`)

	fs := ctx.RootDataset + "/foo bar"
	asnap := fsversion(ctx, fs, "@a snap")
	_, err := zfs.ZFSBookmark(ctx, fs, asnap, "a bookmark")
	if err != nil {
		panic(err)
	}

	type testCase struct {
		description, path string
	}

	cases := []testCase{
		{"snapshot", fs + "@a snap"},
		{"bookmark", fs + "#a bookmark"},
		{"filesystem", fs},
	}

	for i := range cases {
		func() {
			c := cases[i]

			log.Printf("SUBBEGIN testing idempotent destroy %q for path %q", c.description, c.path)

			log.Println("destroy existing")
			err = zfs.ZFSDestroy(ctx, c.path)
			if err != nil {
				panic(err)
			}
			log.Println("destroy again, non-idempotently, must error")
			err = zfs.ZFSDestroy(ctx, c.path)
			if _, ok := err.(*zfs.DatasetDoesNotExist); !ok {
				panic(fmt.Sprintf("%T: %s", err, err))
			}
			log.Println("destroy again, idempotently, must not error")
			err = zfs.ZFSDestroyIdempotent(ctx, c.path)
			if err != nil {
				panic(err)
			}

			log.Println("SUBEND")
		}()
	}

	// also test idempotent destroy for cases where the parent dataset does not exist
	err = zfs.ZFSDestroyIdempotent(ctx, ctx.RootDataset+"/not foo bar@nonexistent snapshot")
	if err != nil {
		panic(err)
	}

	err = zfs.ZFSDestroyIdempotent(ctx, ctx.RootDataset+"/not foo bar#nonexistent bookmark")
	if err != nil {
		panic(err)
	}
}
