package zfs

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dsh2dsh/zrepl/util/chainlock"
)

type mockBatchDestroy struct {
	mtx              chainlock.L
	calls            []string
	commaUnsupported bool
	undestroyable    *regexp.Regexp
	randomerror      string
	e2biglen         int
}

func (m *mockBatchDestroy) DestroySnapshotsCommaSyntaxSupported(_ context.Context) (bool, error) {
	return !m.commaUnsupported, nil
}

func (m *mockBatchDestroy) Destroy(ctx context.Context, args []string) error {
	defer m.mtx.Lock().Unlock()
	if len(args) != 1 {
		panic("unexpected use of Destroy")
	}
	a := args[0]
	if m.e2biglen > 0 && len(a) > m.e2biglen {
		return &os.PathError{Err: syscall.E2BIG} // TestExcessiveArgumentsResultInE2BIG checks that this errors is produced
	}
	m.calls = append(m.calls, a)

	var snapnames []string
	if m.commaUnsupported {
		snapnames = append(snapnames, a)
		if strings.Contains(a, ",") {
			return errors.New("unsupported syntax mock error")
		}
	} else {
		snapnames = append(snapnames, strings.Split(a, ",")...)
	}
	fs, vt, firstsnapname, err := DecomposeVersionString(snapnames[0])
	if err != nil {
		panic(err)
	}
	if vt != Snapshot {
		panic(vt)
	}
	snapnames[0] = firstsnapname

	var undestroyable []string
	if m.undestroyable != nil {
		for _, snap := range snapnames {
			if m.undestroyable.MatchString(snap) {
				undestroyable = append(undestroyable, snap)
			}
		}
	}
	if len(undestroyable) > 0 {
		return &DestroySnapshotsError{
			Filesystem:    fs,
			Undestroyable: undestroyable,
			Reason:        []string{"undestroyable mock with regexp " + m.undestroyable.String()},
		}
	}
	if m.randomerror != "" && strings.Contains(a, m.randomerror) {
		return errors.New("randomerror")
	}
	return nil
}

func TestBatchDestroySnaps(t *testing.T) {
	errs := make([]error, 10)
	nilErrs := func() {
		for i := range errs {
			errs[i] = nil
		}
	}
	opsTemplate := []*DestroySnapOp{
		{"zroot/z", "foo", &errs[0]},
		{"zroot/a", "foo", &errs[1]},
		{"zroot/a", "bar", &errs[2]},
		{"zroot/b", "bar", &errs[3]},
		{"zroot/b", "zab", &errs[4]},
		{"zroot/b", "undestroyable", &errs[5]},
		{"zroot/c", "baz", &errs[6]},
		{"zroot/c", "randomerror", &errs[7]},
		{"zroot/c", "bar", &errs[8]},
		{"zroot/d", "blup", &errs[9]},
	}

	t.Run("single_undestroyable_dataset", func(t *testing.T) {
		nilErrs()
		mock := &mockBatchDestroy{
			commaUnsupported: false,
			undestroyable:    regexp.MustCompile(`undestroyable`),
			randomerror:      "randomerror",
		}

		doDestroy(context.TODO(), opsTemplate, mock)

		require.NoError(t, errs[0])
		require.NoError(t, errs[1])
		require.NoError(t, errs[2])
		require.NoError(t, errs[3])
		require.NoError(t, errs[4])
		require.Error(t, errs[5], "undestroyable")
		require.NoError(t, errs[6])
		require.Error(t, errs[7], "randomerror")
		require.NoError(t, errs[8])
		require.NoError(t, errs[9])

		defer mock.mtx.Lock().Unlock()
		assert.Equal(
			t,
			[]string{
				"zroot/a@bar,foo", // reordered snaps in lexicographical order
				"zroot/b@bar,undestroyable,zab",
				"zroot/b@bar,zab", // eliminate undestroyables, try others again
				"zroot/b@undestroyable",
				"zroot/c@bar,baz,randomerror",
				"zroot/c@bar", // fallback to single-snapshot on non DestroyError
				"zroot/c@baz",
				"zroot/c@randomerror",
				"zroot/d@blup",
				"zroot/z@foo", // ordered at last position
			},
			mock.calls,
		)
	})

	t.Run("all_undestroyable", func(t *testing.T) {
		opsTemplate := []*DestroySnapOp{
			{"zroot/a", "foo", new(error)},
			{"zroot/a", "bar", new(error)},
		}

		mock := &mockBatchDestroy{
			commaUnsupported: false,
			undestroyable:    regexp.MustCompile(`.*`),
		}

		doDestroy(context.TODO(), opsTemplate, mock)

		defer mock.mtx.Lock().Unlock()
		assert.Equal(
			t,
			[]string{
				"zroot/a@bar,foo", // reordered snaps in lexicographical order
				"zroot/a@bar",
				"zroot/a@foo",
			},
			mock.calls,
		)
	})

	t.Run("comma_syntax_unsupported", func(t *testing.T) {
		nilErrs()
		mock := &mockBatchDestroy{
			commaUnsupported: true,
			undestroyable:    regexp.MustCompile(`undestroyable`),
			randomerror:      "randomerror",
		}

		doDestroy(context.TODO(), opsTemplate, mock)

		assert.NoError(t, errs[0])
		assert.NoError(t, errs[1])
		assert.NoError(t, errs[2])
		assert.NoError(t, errs[3])
		assert.NoError(t, errs[4])
		require.Error(t, errs[5], "undestroyable")
		require.NoError(t, errs[6])
		require.Error(t, errs[7], "randomerror")
		assert.NoError(t, errs[8])
		assert.NoError(t, errs[9])

		defer mock.mtx.Lock().Unlock()
		assert.Equal(
			t,
			[]string{
				// expect exactly argument order
				"zroot/z@foo",
				"zroot/a@foo",
				"zroot/a@bar",
				"zroot/b@bar",
				"zroot/b@zab",
				"zroot/b@undestroyable",
				"zroot/c@baz",
				"zroot/c@randomerror",
				"zroot/c@bar",
				"zroot/d@blup",
			},
			mock.calls,
		)
	})

	t.Run("empty_ops", func(t *testing.T) {
		mock := &mockBatchDestroy{}
		doDestroy(context.TODO(), nil, mock)
		defer mock.mtx.Lock().Unlock()
		assert.Empty(t, mock.calls)
	})

	t.Run("ops_without_snapnames", func(t *testing.T) {
		mock := &mockBatchDestroy{}
		var err error
		ops := []*DestroySnapOp{{"somefs", "", &err}}
		doDestroy(context.TODO(), ops, mock)
		require.Error(t, err)
		defer mock.mtx.Lock().Unlock()
		assert.Empty(t, mock.calls)
	})

	t.Run("ops_without_fsnames", func(t *testing.T) {
		mock := &mockBatchDestroy{}
		var err error
		ops := []*DestroySnapOp{{"", "fsname", &err}}
		doDestroy(context.TODO(), ops, mock)
		require.Error(t, err)
		defer mock.mtx.Lock().Unlock()
		assert.Empty(t, mock.calls)
	})

	t.Run("splits_up_batches_at_e2big", func(t *testing.T) {
		mock := &mockBatchDestroy{
			e2biglen: 10,
		}

		var dummy error
		reqs := []*DestroySnapOp{
			// should fit (1111@a,b,c)
			{"1111", "a", &dummy},
			{"1111", "b", &dummy},
			{"1111", "c", &dummy},

			// should split
			{"2222", "01", &dummy},
			{"2222", "02", &dummy},
			{"2222", "03", &dummy},
			{"2222", "04", &dummy},
			{"2222", "05", &dummy},
			{"2222", "06", &dummy},
			{"2222", "07", &dummy},
			{"2222", "08", &dummy},
			{"2222", "09", &dummy},
			{"2222", "10", &dummy},
		}

		doDestroy(context.TODO(), reqs, mock)

		defer mock.mtx.Lock().Unlock()
		assert.Equal(
			t,
			[]string{
				"1111@a,b,c",
				"2222@01,02",
				"2222@03",
				"2222@04,05",
				"2222@06,07",
				"2222@08",
				"2222@09,10",
			},
			mock.calls,
		)
	})
}

func TestExcessiveArgumentsResultInE2BIG(t *testing.T) {
	// FIXME dynamic value
	const maxArgumentLength = 1 << 20 // higher than any OS we know, should always fail
	t.Logf("maxArgumentLength=%v", maxArgumentLength)
	maxArg := bytes.Repeat([]byte("a"), maxArgumentLength)
	cmd := exec.Command("/bin/sh", "-c", "echo -n $1; echo -n $2", "cmdname", string(maxArg), string(maxArg))
	output, err := cmd.CombinedOutput()
	if pe, ok := err.(*os.PathError); ok && pe.Err == syscall.E2BIG {
		t.Logf("ok, system returns E2BIG")
	} else {
		t.Errorf("system did not return E2BIG, but err=%T: %v ", err, err)
		t.Logf("output:\n%s", output)
	}
}
