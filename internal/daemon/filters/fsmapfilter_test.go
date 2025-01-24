package filters

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dsh2dsh/zrepl/internal/zfs"
)

func TestDatasetMapFilter(t *testing.T) {
	type testCase struct {
		name   string
		filter map[string]string
		// each entry is checked to match the filter's `pass` return value
		checkPass map[string]bool
	}

	tcs := []testCase{
		{
			"default_no_match",
			map[string]string{},
			map[string]bool{
				"":      false,
				"foo":   false,
				"zroot": false,
			},
		},
		{
			"more_specific_path_has_precedence",
			map[string]string{
				"tank<":         "ok",
				"tank/tmp<":     "!",
				"tank/home/x<":  "!",
				"tank/home/x/1": "ok",
			},
			map[string]bool{
				"zroot":         false,
				"tank":          true,
				"tank/tmp":      false,
				"tank/tmp/foo":  false,
				"tank/home/x":   false,
				"tank/home/y":   true,
				"tank/home/x/1": true,
				"tank/home/x/2": false,
			},
		},
		{
			"precedence_of_specific_over_subtree_wildcard_on_same_path",
			map[string]string{
				"tank/home/bob":  "ok",
				"tank/home/bob<": "!",
			},
			map[string]bool{
				"tank/home/bob":           true,
				"tank/home/bob/downloads": false,
			},
		},
		{
			name: "with shell patterns",
			filter: map[string]string{
				"tank/home</*/foo": "ok",
				"tank/home/mark<":  "!",
			},
			checkPass: map[string]bool{
				"tank/home":              false,
				"tank/home/bob":          false,
				"tank/home/bob/foo":      true,
				"tank/home/alice/foo":    true,
				"tank/home/john/foo/bar": false,
				"tank/home/john/bar":     false,
				"tank/home/mark/foo":     false,
			},
		},
		{
			name: "include first level",
			filter: map[string]string{
				"test/app</*": "ok",
			},
			checkPass: map[string]bool{
				"test/app/1":       true,
				"test/app/1/cache": false,
				"test/app/2":       true,
				"test/app/2/cache": false,
			},
		},
		{
			name: "exclude by shell pattern",
			filter: map[string]string{
				"test/app</*/cache": "!",
			},
			checkPass: map[string]bool{
				"test/app/1":       true,
				"test/app/1/cache": false,
				"test/app/2":       true,
				"test/app/2/cache": false,
			},
		},
		{
			name: "match all",
			filter: map[string]string{
				"<": "ok",
			},
			checkPass: map[string]bool{
				"test":             true,
				"test/app":         true,
				"test/app/2":       true,
				"test/app/2/cache": true,
			},
		},
	}

	for tc := range tcs {
		t.Run(tcs[tc].name, func(t *testing.T) {
			c := tcs[tc]
			f := New(len(c.filter))
			for p, a := range c.filter {
				err := f.Add(p, a)
				if err != nil {
					t.Fatalf("incorrect filter spec: %s", err)
				}
			}
			f.CompatSort()
			for p, checkPass := range c.checkPass {
				zp, err := zfs.NewDatasetPath(p)
				if err != nil {
					t.Fatalf("incorrect path spec: %s", err)
				}
				pass, err := f.Filter(zp)
				if err != nil {
					t.Fatalf("unexpected filter error: %s", err)
				}
				ok := pass == checkPass
				failstr := "OK"
				if !ok {
					failstr = "FAIL"
					t.Fail()
				}
				t.Logf("%-40q  %5v  (exp=%v act=%v)", p, failstr, checkPass, pass)
			}
		})
	}
}

func TestNoFilter(t *testing.T) {
	f, err := NoFilter()
	require.NoError(t, err)
	require.NotNil(t, f)

	tests := []string{
		"test/foo/bar/baz",
		"test/foo/bar",
		"test/foo",
		"test",
	}

	for _, s := range tests {
		t.Run(s, func(t *testing.T) {
			path, err := zfs.NewDatasetPath(s)
			require.NoError(t, err)
			require.NotNil(t, path)

			ok, err := f.Filter(path)
			require.NoError(t, err)
			assert.True(t, ok)
		})
	}
}
