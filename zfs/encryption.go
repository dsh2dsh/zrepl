package zfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"sync"

	"github.com/dsh2dsh/zrepl/util/envconst"
	"github.com/dsh2dsh/zrepl/zfs/zfscmd"
)

var encryptionCLISupport struct {
	once      sync.Once
	supported bool
	err       error
}

func EncryptionCLISupported(ctx context.Context) (bool, error) {
	encryptionCLISupport.once.Do(func() {
		// "feature discovery"
		cmd := zfscmd.CommandContext(ctx, ZfsBin, "load-key").WithLogError(false)
		output, err := cmd.CombinedOutput()
		if err != nil {
			var exitError *exec.ExitError
			if !errors.As(err, &exitError) || !exitError.Exited() {
				encryptionCLISupport.err = fmt.Errorf(
					"native encryption cli support feature check failed: %w", err)
			}
		}
		def := bytes.Contains(output, []byte("load-key")) &&
			bytes.Contains(output, []byte("keylocation"))
		if err != nil {
			cmd.LogError(err, def)
		}
		encryptionCLISupport.supported = envconst.Bool(
			"ZREPL_EXPERIMENTAL_ZFS_ENCRYPTION_CLI_SUPPORTED", def)
		debug("encryption cli feature check complete %#v", &encryptionCLISupport)
	})
	return encryptionCLISupport.supported, encryptionCLISupport.err
}

// returns false, nil if encryption is not supported
func ZFSGetEncryptionEnabled(ctx context.Context, fs string,
) (enabled bool, err error) {
	defer func(e *error) {
		if *e != nil {
			*e = fmt.Errorf("zfs get encryption enabled fs=%q: %s", fs, *e)
		}
	}(&err)

	if supp, err := EncryptionCLISupported(ctx); err != nil {
		return false, err
	} else if !supp {
		return false, nil
	} else if err := validateZFSFilesystem(fs); err != nil {
		return false, err
	}

	props, err := zfsGet(ctx, fs, []string{"encryption"}, SourceAny)
	if err != nil {
		return false, fmt.Errorf("cannot get `encryption` property: %w", err)
	}

	switch props.Get("encryption") {
	case "":
		panic("zfs get should return a value for `encryption`")
	case "-":
		return false, errors.New("`encryption` property should never be \"-\"")
	case "off":
		return false, nil
	}

	// we don't want to hardcode the cipher list, and we checked for != 'off'
	// ==> assume any other value means encryption is enabled
	// TODO add test to OpenZFS test suite
	return true, nil
}
