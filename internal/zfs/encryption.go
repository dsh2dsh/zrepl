package zfs

import (
	"context"
	"errors"
	"fmt"
)

// returns false, nil if encryption is not supported
func ZFSGetEncryptionEnabled(ctx context.Context, fs string,
) (enabled bool, err error) {
	defer func(e *error) {
		if *e != nil {
			*e = fmt.Errorf("zfs get encryption enabled fs=%q: %w", fs, *e)
		}
	}(&err)

	if err := validateZFSFilesystem(fs); err != nil {
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
