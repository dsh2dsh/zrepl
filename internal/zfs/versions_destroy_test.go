package zfs

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"syscall"
	"testing"
)

func TestExcessiveArgumentsResultInE2BIG(t *testing.T) {
	// FIXME dynamic value
	const maxArgumentLength = 1 << 20 // higher than any OS we know, should always fail
	t.Logf("maxArgumentLength=%v", maxArgumentLength)
	maxArg := bytes.Repeat([]byte("a"), maxArgumentLength)
	cmd := exec.Command("/bin/sh", "-c", "echo -n $1; echo -n $2", "cmdname", string(maxArg), string(maxArg))
	output, err := cmd.CombinedOutput()
	pe, ok := errors.AsType[*os.PathError](err)
	if ok && errors.Is(pe.Err, syscall.E2BIG) {
		t.Logf("ok, system returns E2BIG")
	} else {
		t.Errorf("system did not return E2BIG, but err=%T: %v ", err, err)
		t.Logf("output:\n%s", output)
	}
}
