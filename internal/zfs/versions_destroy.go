package zfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"syscall"
)

func ZFSDestroyFilesystemVersion(ctx context.Context, filesystem *DatasetPath,
	version *FilesystemVersion,
) error {
	fullName := version.ToAbsPath(filesystem)
	// Sanity check...
	if !strings.ContainsAny(fullName, "@#") {
		return fmt.Errorf(
			"sanity check failed: no @ or # character found in %q", fullName)
	}
	return ZFSDestroy(ctx, fullName)
}

type DestroySnapOp struct {
	Name string
	Err  error
}

func (self *DestroySnapOp) String() string {
	return "destroy operation @" + self.Name
}

func ZFSDestroyFilesystemVersions(ctx context.Context, fs string,
	reqs []DestroySnapOp,
) {
	doDestroy(ctx, fs, reqs)
}

func setDestroySnapOpErr(b []*DestroySnapOp, err error) {
	for _, r := range b {
		r.Err = err
	}
}

func doDestroy(ctx context.Context, fs string, reqs []DestroySnapOp) {
	validated := make([]*DestroySnapOp, 0, len(reqs))
	for i := range reqs {
		r := &reqs[i]
		// Filesystem and Snapshot should not be empty. ZFS will generally fail
		// because those are invalid destroy arguments, but we'd rather apply
		// defensive programming here (doing destroy after all).
		switch {
		case fs == "":
			r.Err = errors.New("Filesystem must not be an empty string")
		case r.Name == "":
			r.Err = errors.New("Name must not be an empty string")
		default:
			validated = append(validated, r)
		}
	}
	doDestroyBatched(ctx, fs, validated)
}

func doDestroySeq(ctx context.Context, fs string, reqs []*DestroySnapOp) {
	for _, r := range reqs {
		r.Err = ZFSDestroy(ctx, fs+"@"+r.Name)
	}
}

// batch must be on same Filesystem, panics otherwise
func tryBatch(ctx context.Context, fs string, batch []*DestroySnapOp) error {
	if len(batch) == 0 {
		return nil
	}
	batchNames := make([]string, len(batch))
	for i := range batchNames {
		batchNames[i] = batch[i].Name
	}
	return ZFSDestroy(ctx, fs+"@"+strings.Join(batchNames, ","))
}

// fsbatch must be on same filesystem
func doDestroyBatched(ctx context.Context, fs string, fsbatch []*DestroySnapOp,
) {
	if len(fsbatch) < 2 {
		doDestroySeq(ctx, fs, fsbatch)
		return
	}

	err := tryBatch(ctx, fs, fsbatch)
	if err != nil {
		pe, ok := errors.AsType[*os.PathError](err)
		if ok && errors.Is(pe.Err, syscall.E2BIG) {
			// See TestExcessiveArgumentsResultInE2BIG. Try halving batch size,
			// assuming snapshots names are roughly the same length.
			debug("batch destroy: E2BIG encountered: %s", err)
			for chunk := range slices.Chunk(fsbatch, len(fsbatch)/2) {
				doDestroyBatched(ctx, fs, chunk)
			}
			return
		}
	} else {
		setDestroySnapOpErr(fsbatch, nil)
		return
	}

	// the destroys that will be tried sequentially after "smart" error handling
	// below
	seqSnaps := fsbatch

	if errDestroy, ok := errors.AsType[*DestroySnapshotsError](err); ok {
		// eliminate undestroyable datasets from batch and try it once again
		strippedBatch := make([]*DestroySnapOp, 0, len(fsbatch))
		remaining := make([]*DestroySnapOp, 0, len(fsbatch))

		undestroyable := make(map[string]struct{}, len(errDestroy.Undestroyable))
		for _, name := range errDestroy.Undestroyable {
			undestroyable[name] = struct{}{}
		}
		for _, b := range fsbatch {
			if _, ok := undestroyable[b.Name]; ok {
				remaining = append(remaining, b)
			} else {
				strippedBatch = append(strippedBatch, b)
			}
		}

		err := tryBatch(ctx, fs, strippedBatch)
		if err != nil {
			// Run entire batch sequentially if the stripped one fails. It shouldn't
			// because we stripped erroneous datasets.
			seqSnaps = fsbatch // shadow
		} else {
			setDestroySnapOpErr(strippedBatch, nil) // these ones worked
			seqSnaps = remaining                    // shadow
		}
		// fallthrough
	}
	doDestroySeq(ctx, fs, seqSnaps)
}
