package zfs

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"strings"

	"github.com/dsh2dsh/zrepl/internal/zfs/zfscmd"
)

func NewListCmd(ctx context.Context, props, zfsArgs []string) *zfscmd.Cmd {
	args := make([]string, 0, 4+len(zfsArgs))
	args = append(args, "list", "-H", "-p", "-o", strings.Join(props, ","))
	args = append(args, zfsArgs...)
	return zfscmd.CommandContext(ctx, ZfsBin, args...)
}

func ListIter(ctx context.Context, properties []string,
	notExistHint *DatasetPath, cmd *zfscmd.Cmd,
) iter.Seq2[[]string, error] {
	cmd.WithLogError(false)
	var stderrBuf bytes.Buffer
	stdout, err := cmd.StdoutPipeWithErrorBuf(&stderrBuf)

	iter := func(yield func([]string, error) bool) {
		if err != nil {
			yield(nil, err)
			return
		} else if err := cmd.Start(); err != nil {
			yield(nil, err)
			return
		}

		err = scanCmdOutput(cmd, stdout, &stderrBuf,
			func(s string) (error, bool) {
				fields := strings.SplitN(s, "\t", len(properties)+1)
				if len(fields) != len(properties) {
					return fmt.Errorf("unexpected output from zfs list: %q", s), false
				} else if ctx.Err() != nil {
					return nil, false
				}
				return nil, yield(fields, nil)
			})
		if err != nil {
			if notExistHint != nil {
				err = maybeDatasetNotExists(cmd, notExistHint.ToString(), err)
			} else {
				cmd.WithStderrOutput(stderrBuf.Bytes()).LogError(err, false)
			}
			yield(nil, err)
		}
	}
	return iter
}

func scanCmdOutput(cmd *zfscmd.Cmd, r io.Reader, stderrBuf *bytes.Buffer,
	fn func(s string) (error, bool),
) (err error) {
	var ok bool
	s := bufio.NewScanner(r)
	for s.Scan() {
		if err, ok = fn(s.Text()); err != nil || !ok {
			break
		}
	}

	if err != nil || !ok || s.Err() != nil {
		_, _ = io.Copy(io.Discard, r)
	}
	cmdErr := cmd.Wait()

	switch {
	case err != nil:
	case s.Err() != nil:
		err = s.Err()
	case cmdErr != nil:
		err = NewZfsError(cmdErr, stderrBuf.Bytes())
	}
	return err
}

func maybeDatasetNotExists(cmd *zfscmd.Cmd, path string, err error) error {
	zfsError, ok := errors.AsType[*ZFSError](err)
	if !ok {
		return err
	}

	if len(zfsError.Stderr) != 0 {
		cmd.WithStderrOutput(zfsError.Stderr)
		enotexist := tryDatasetDoesNotExist(path, zfsError.Stderr)
		if enotexist != nil {
			cmd.LogError(err, true)
			return enotexist
		}
	}

	cmd.LogError(err, false)
	return err
}

func listVersions(ctx context.Context, props []string, fs *DatasetPath,
	cmd *zfscmd.Cmd,
) ([]FilesystemVersion, error) {
	snaps := []FilesystemVersion{}
	listResults := ListIter(ctx, props, fs, cmd)
	for fields, err := range listResults {
		if err != nil {
			return nil, err
		}
		var args ParseFilesystemVersionArgs
		v, err := args.
			WithFullName(fields[0]).
			WithGuid(fields[1]).
			WithCreateTxg(fields[2]).
			WithCreation(fields[3]).
			WithUserRefs(fields[4]).
			Parse()
		if err != nil {
			return nil, err
		}
		snaps = append(snaps, v)
	}
	return snaps, nil
}
