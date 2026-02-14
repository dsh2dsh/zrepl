package zfs

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"

	"github.com/dsh2dsh/zrepl/internal/zfs/zfscmd"
)

// NOTE: Update ZFSSendARgs.Validate when changing fields (potentially SECURITY SENSITIVE)
type ResumeToken struct {
	HasFromGUID, HasToGUID        bool
	FromGUID, ToGUID              uint64
	ToName                        string
	HasCompressOK, CompressOK     bool
	HasRawOk, RawOK               bool
	HasLargeBlockOK, LargeBlockOK bool
	HasEmbedOk, EmbedOK           bool
	HasSavedOk, SavedOk           bool
}

var resumeTokenNVListRE = regexp.MustCompile(`\t(\S+) = (.*)`)

var (
	resumeTokenContentsRE  = regexp.MustCompile(`resume token contents:\nnvlist version: 0`)
	resumeTokenIsCorruptRE = regexp.MustCompile(`resume token is corrupt`)
)

var (
	ResumeTokenCorruptError         = errors.New("resume token is corrupt")
	ResumeTokenDecodingNotSupported = errors.New("zfs binary does not allow decoding resume token or zrepl cannot scrape zfs output")
	ResumeTokenParsingError         = errors.New("zrepl cannot parse resume token values")
)

// Abuse 'zfs send' to decode the resume token
//
// FIXME: implement nvlist unpacking in Go and read through libzfs_sendrecv.c
//
// Example resume tokens:
//
// # From a non-incremental send
//
// 1-bf31b879a-b8-789c636064000310a500c4ec50360710e72765a5269740f80cd8e4d3d28a534b18e00024cf86249f5459925acc802a8facbf243fbd3433858161f5ddb9ab1ae7c7466a20c97382e5f312735319180af2f3730cf58166953824c2cc0200cde81651
//
// # From an incremental send
//
// 1-c49b979a2-e0-789c636064000310a501c49c50360710a715e5e7a69766a63040c1eabb735735ce8f8d5400b2d991d4e52765a5269740f82080219f96569c5ac2000720793624f9a4ca92d46206547964fd25f91057f09e37babb88c9bf5503499e132c9f97989bcac050909f9f63a80f34abc421096616007c881d4c
//
// Resulting output of zfs send -nvt <token>
//
// resume token contents:
// nvlist version: 0
//
//	fromguid = 0x595d9f81aa9dddab
//	object = 0x1
//	offset = 0x0
//	bytes = 0x0
//	toguid = 0x854f02a2dd32cf0d
//	toname = pool1/test@b
//
// cannot resume send: 'pool1/test@b' used in the initial send no longer exists
func ParseResumeToken(ctx context.Context, token string) (*ResumeToken, error) {
	cmd := zfscmd.CommandContext(ctx, ZfsBin, "send", "-nvt", token).
		WithLogError(false)
	output, err := cmd.CombinedOutput()
	if err != nil {
		exitError, ok := errors.AsType[*exec.ExitError](err)
		if !ok || !exitError.Exited() {
			cmd.LogError(err, false)
			return nil, err
		}
		// we abuse zfs send for decoding, the exit error may be due to
		//
		//  a) the token being from a third machine
		//  b) it no longer exists on the machine where
	}

	if !resumeTokenContentsRE.Match(output) {
		if resumeTokenIsCorruptRE.Match(output) {
			return nil, ResumeTokenCorruptError
		}
		return nil, ResumeTokenDecodingNotSupported
	}

	matches := resumeTokenNVListRE.FindAllStringSubmatch(string(output), -1)
	if matches == nil {
		return nil, ResumeTokenDecodingNotSupported
	}

	rt := &ResumeToken{}
	for _, m := range matches {
		attr, val := m[1], m[2]
		switch attr {
		case "fromguid":
			rt.FromGUID, err = strconv.ParseUint(val, 0, 64)
			if err != nil {
				return nil, ResumeTokenParsingError
			}
			rt.HasFromGUID = true
		case "toguid":
			rt.ToGUID, err = strconv.ParseUint(val, 0, 64)
			if err != nil {
				return nil, ResumeTokenParsingError
			}
			rt.HasToGUID = true
		case "toname":
			rt.ToName = val
		case "rawok":
			rt.HasRawOk = true
			rt.RawOK, err = strconv.ParseBool(val)
			if err != nil {
				return nil, ResumeTokenParsingError
			}
		case "compressok":
			rt.HasCompressOK = true
			rt.CompressOK, err = strconv.ParseBool(val)
			if err != nil {
				return nil, ResumeTokenParsingError
			}
		case "embedok":
			rt.HasEmbedOk = true
			rt.EmbedOK, err = strconv.ParseBool(val)
			if err != nil {
				return nil, ResumeTokenParsingError
			}
		case "largeblockok":
			rt.HasLargeBlockOK = true
			rt.LargeBlockOK, err = strconv.ParseBool(val)
			if err != nil {
				return nil, ResumeTokenParsingError
			}
		case "savedok":
			rt.HasSavedOk = true
			rt.SavedOk, err = strconv.ParseBool(val)
			if err != nil {
				return nil, ResumeTokenParsingError
			}
		}
	}

	if !rt.HasToGUID {
		return nil, ResumeTokenDecodingNotSupported
	}
	return rt, nil
}

func (t *ResumeToken) ToNameSplit() (fs *DatasetPath, snapName string, err error) {
	comps := strings.SplitN(t.ToName, "@", 2)
	if len(comps) != 2 {
		return nil, "", fmt.Errorf("resume token field `toname` does not contain @: %q", t.ToName)
	}
	dp, err := NewDatasetPath(comps[0])
	if err != nil {
		return nil, "", fmt.Errorf("resume token field `toname` dataset path invalid: %w", err)
	}
	return dp, comps[1], nil
}
