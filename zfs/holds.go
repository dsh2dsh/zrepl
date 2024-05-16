package zfs

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"syscall"

	"github.com/dsh2dsh/zrepl/util/envconst"
	"github.com/dsh2dsh/zrepl/zfs/zfscmd"
)

// no need for feature tests, holds have been around forever

func validateNotEmpty(field, s string) error {
	if s == "" {
		return fmt.Errorf("`%s` must not be empty", field)
	}
	return nil
}

// returned err != nil is guaranteed to represent invalid hold tag
func ValidHoldTag(tag string) error {
	maxlen := envconst.Int("ZREPL_ZFS_MAX_HOLD_TAG_LEN", 256-1) // 256 include NULL byte, from module/zfs/dsl_userhold.c
	if len(tag) > maxlen {
		return fmt.Errorf("hold tag %q exceeds max length of %d", tag, maxlen)
	}
	return nil
}

// Idemptotent: does not return an error if the tag already exists
func ZFSHold(ctx context.Context, fs string, v FilesystemVersion, tag string,
) error {
	if !v.IsSnapshot() {
		return fmt.Errorf("can only hold snapshots, got %s", v.RelName())
	} else if err := validateNotEmpty("tag", tag); err != nil {
		return err
	}

	fullPath := v.FullPath(fs)
	cmd := zfscmd.CommandContext(ctx, ZfsBin, "hold", tag, fullPath).
		WithLogError(false)
	output, err := cmd.CombinedOutput()
	if err != nil {
		if bytes.Contains(output, []byte("tag already exists on this dataset")) {
			cmd.LogError(err, true)
			return nil
		}
		cmd.LogError(err, false)
		return &ZFSError{output, fmt.Errorf("cannot hold %q: %w", fullPath, err)}
	}
	return nil
}

func ZFSHolds(ctx context.Context, fs, snap string) ([]string, error) {
	if err := validateZFSFilesystem(fs); err != nil {
		return nil, fmt.Errorf("`fs` is not a valid filesystem path: %w", err)
	} else if snap == "" {
		return nil, fmt.Errorf("`snap` must not be empty")
	}

	dp := fmt.Sprintf("%s@%s", fs, snap)
	cmd := zfscmd.CommandContext(ctx, ZfsBin, "holds", "-H", dp)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, &ZFSError{output, fmt.Errorf("zfs holds failed: %w", err)}
	}
	scan := bufio.NewScanner(bytes.NewReader(output))
	var tags []string
	for scan.Scan() {
		// NAME              TAG  TIMESTAMP
		comps := strings.SplitN(scan.Text(), "\t", 3)
		if len(comps) != 3 {
			return nil, fmt.Errorf("zfs holds: unexpected output\n%s", output)
		}
		if comps[0] != dp {
			return nil, fmt.Errorf("zfs holds: unexpected output: expecting %q as first component, got %q\n%s", dp, comps[0], output)
		}
		tags = append(tags, comps[1])
	}
	return tags, nil
}

// Idempotent: if the hold doesn't exist, this is not an error
func ZFSRelease(ctx context.Context, tag string, snaps ...string) error {
	cumLens := make([]int, len(snaps))
	for i := 1; i < len(snaps); i++ {
		cumLens[i] = cumLens[i-1] + len(snaps[i])
	}
	maxInvocationLen := 12 * os.Getpagesize()
	var noSuchTagLines, otherLines []string
	for i := 0; i < len(snaps); {
		j := i
		for ; j < len(snaps); j++ {
			if cumLens[j]-cumLens[i] > maxInvocationLen {
				break
			}
		}

		args := []string{"release", tag}
		args = append(args, snaps[i:j]...)
		cmd := zfscmd.CommandContext(ctx, ZfsBin, args...).WithLogError(false)
		output, err := cmd.CombinedOutput()
		if pe, ok := err.(*os.PathError); err != nil && ok && pe.Err == syscall.E2BIG {
			maxInvocationLen = maxInvocationLen / 2
			cmd.LogError(err, true)
			continue
		}
		// further error handling part of error scraper below

		maxInvocationLen = maxInvocationLen + os.Getpagesize()
		i = j

		// even if release fails for datasets where there's no hold with the tag
		// the hold is still released on datasets which have a hold with the tag
		// FIXME verify this in a platformtest
		// => screen-scrape
		scan := bufio.NewScanner(bytes.NewReader(output))
		var hasOtherLines bool
		for scan.Scan() {
			line := scan.Text()
			if strings.Contains(line, "no such tag on this dataset") {
				noSuchTagLines = append(noSuchTagLines, line)
			} else {
				otherLines = append(otherLines, line)
				hasOtherLines = true
			}
		}

		if err != nil {
			cmd.LogError(err, !hasOtherLines)
		}
	}
	if debugEnabled {
		debug("zfs release: no such tag lines=%v otherLines=%v", noSuchTagLines, otherLines)
	}
	if len(otherLines) > 0 {
		return fmt.Errorf("unknown zfs error while releasing hold with tag %q:\n%s", tag, strings.Join(otherLines, "\n"))
	}
	return nil
}
