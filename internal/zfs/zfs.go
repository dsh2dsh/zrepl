package zfs

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/dsh2dsh/zrepl/internal/util/nodefault"
	zfsprop "github.com/dsh2dsh/zrepl/internal/zfs/property"
	"github.com/dsh2dsh/zrepl/internal/zfs/zfscmd"
)

var ZfsBin string = "zfs"

type DatasetPath struct {
	comps []string
}

func (p *DatasetPath) ToString() string {
	return strings.Join(p.comps, "/")
}

func (p *DatasetPath) Empty() bool {
	return len(p.comps) == 0
}

func (p *DatasetPath) Extend(extend *DatasetPath) {
	p.comps = append(p.comps, extend.comps...)
}

func (p *DatasetPath) HasPrefix(prefix *DatasetPath) bool {
	if len(prefix.comps) > len(p.comps) {
		return false
	}
	for i := range prefix.comps {
		if prefix.comps[i] != p.comps[i] {
			return false
		}
	}
	return true
}

func (p *DatasetPath) TrimPrefix(prefix *DatasetPath) {
	if !p.HasPrefix(prefix) {
		return
	}
	prelen := len(prefix.comps)
	newlen := len(p.comps) - prelen
	oldcomps := p.comps
	p.comps = make([]string, newlen)
	for i := 0; i < newlen; i++ {
		p.comps[i] = oldcomps[prelen+i]
	}
}

func (p *DatasetPath) TrimNPrefixComps(n int) {
	if len(p.comps) < n {
		n = len(p.comps)
	}
	if n == 0 {
		return
	}
	p.comps = p.comps[n:]
}

func (p DatasetPath) Equal(q *DatasetPath) bool {
	if len(p.comps) != len(q.comps) {
		return false
	}
	for i := range p.comps {
		if p.comps[i] != q.comps[i] {
			return false
		}
	}
	return true
}

func (p *DatasetPath) Length() int {
	return len(p.comps)
}

func (p *DatasetPath) Copy() (c *DatasetPath) {
	c = &DatasetPath{}
	c.comps = make([]string, len(p.comps))
	copy(c.comps, p.comps)
	return
}

func (p *DatasetPath) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.comps)
}

func (p *DatasetPath) UnmarshalJSON(b []byte) error {
	p.comps = make([]string, 0)
	return json.Unmarshal(b, &p.comps)
}

func (p *DatasetPath) Pool() (string, error) {
	if len(p.comps) < 1 {
		return "", errors.New("dataset path does not have a pool component")
	}
	return p.comps[0], nil
}

func NewDatasetPath(s string) (p *DatasetPath, err error) {
	p = &DatasetPath{}
	if s == "" {
		p.comps = make([]string, 0)
		return p, nil // the empty dataset path
	}
	const FORBIDDEN = "@#|\t<>*"
	/* Documentation of allowed characters in zfs names:
	https://docs.oracle.com/cd/E19253-01/819-5461/gbcpt/index.html
	Space is missing in the oracle list, but according to
	https://github.com/zfsonlinux/zfs/issues/439
	there is evidence that it was intentionally allowed
	*/
	if strings.ContainsAny(s, FORBIDDEN) {
		err = fmt.Errorf("contains forbidden characters (any of '%s')", FORBIDDEN)
		return
	}
	p.comps = strings.Split(s, "/")
	if p.comps[len(p.comps)-1] == "" {
		err = errors.New("must not end with a '/'")
		return
	}
	return
}

func toDatasetPath(s string) *DatasetPath {
	p, err := NewDatasetPath(s)
	if err != nil {
		panic(err)
	}
	return p
}

func NewZfsError(err error, stderr []byte) *ZFSError {
	if len(stderr) == 0 {
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			stderr = exitError.Stderr
		}

	}
	return &ZFSError{Stderr: stderr, WaitErr: err}
}

type ZFSError struct {
	Stderr  []byte
	WaitErr error
}

func (self *ZFSError) Error() string {
	msg := "zfs exited with error: " + self.WaitErr.Error()
	if len(self.Stderr) == 0 {
		return msg
	}

	firstLine, leftBytes, _ := bytes.Cut(self.Stderr, []byte{'\n'})
	msg += ": " + string(firstLine)
	if len(leftBytes) != 0 {
		return msg + fmt.Sprintf(": %d bytes left", len(leftBytes))
	}
	return msg
}

func (self *ZFSError) Unwrap() error {
	return self.WaitErr
}

func ZFSList(ctx context.Context, properties []string, zfsArgs ...string,
) ([][]string, error) {
	res := [][]string{}
	for r := range ZFSListIter(ctx, properties, nil, zfsArgs...) {
		if r.Err != nil {
			return nil, r.Err
		}
		res = append(res, r.Fields)
	}
	return res, nil
}

type ZFSListResult struct {
	Fields []string
	Err    error
}

// ZFSListIter executes `zfs list` and returns an iterator with the results.
//
// If notExistHint is not nil and zfs exits with an error, the stderr is
// attempted to be interpreted as a *DatasetDoesNotExist error.
func ZFSListIter(ctx context.Context, properties []string,
	notExistHint *DatasetPath, zfsArgs ...string,
) iter.Seq[ZFSListResult] {
	cmd := zfscmd.CommandContext(ctx, ZfsBin,
		zfsListArgs(properties, zfsArgs)...).WithLogError(false)
	var stderrBuf bytes.Buffer
	stdout, err := cmd.StdoutPipeWithErrorBuf(&stderrBuf)

	iter := func(yield func(ZFSListResult) bool) {
		if err != nil {
			yield(ZFSListResult{Err: err})
			return
		} else if err := cmd.Start(); err != nil {
			yield(ZFSListResult{Err: err})
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
				return nil, yield(ZFSListResult{Fields: fields})
			})
		if err != nil {
			if notExistHint != nil {
				err = maybeDatasetNotExists(cmd, notExistHint.ToString(), err)
			} else {
				cmd.WithStderrOutput(stderrBuf.Bytes()).LogError(err, false)
			}
			yield(ZFSListResult{Err: err})
		}
	}
	return iter
}

func zfsListArgs(properties []string, zfsArgs []string) []string {
	args := make([]string, 0, 4+len(zfsArgs))
	args = append(args, "list", "-H", "-p", "-o", strings.Join(properties, ","))
	args = append(args, zfsArgs...)
	return args
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
	return
}

func maybeDatasetNotExists(cmd *zfscmd.Cmd, path string, err error) error {
	var zfsError *ZFSError
	if !errors.As(err, &zfsError) {
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

// FIXME replace with EntityNamecheck
func validateZFSFilesystem(fs string) error {
	if len(fs) < 1 {
		return errors.New("filesystem path must have length > 0")
	}
	return nil
}

// v must not be nil and be already validated
func absVersion(fs string, v *ZFSSendArgVersion) (full string, err error) {
	if err := validateZFSFilesystem(fs); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s%s", fs, v.RelName), nil
}

func NewSendStream(cmd *zfscmd.Cmd, r io.ReadCloser, stderrBuf *bytes.Buffer,
	cancel context.CancelFunc,
) *SendStream {
	return &SendStream{
		cmd:          cmd,
		stdoutReader: r,
		stderrBuf:    stderrBuf,
		cancel:       cancel,
	}
}

type sendStreamState int

const (
	sendStreamOpen sendStreamState = iota
	sendStreamClosed
)

type SendStream struct {
	cmd          *zfscmd.Cmd
	stdoutReader io.ReadCloser // not *os.File for mocking during platformtest
	stderrBuf    *bytes.Buffer
	cancel       context.CancelFunc

	mtx      sync.Mutex
	state    sendStreamState
	exitErr  *ZFSError
	testMode bool
}

func (s *SendStream) Read(p []byte) (int, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.state == sendStreamClosed {
		return 0, os.ErrClosed
	} else if s.state != sendStreamOpen {
		panic("unreachable")
	}

	n, err := s.stdoutReader.Read(p)
	if err == nil {
		return n, err
	}

	debug("sendStream: read: err=%T %s", err, err)
	if err == io.EOF {
		// io.EOF must be bubbled up as is so that consumers can handle it
		// properly.
		return n, err
	} else if !s.testMode {
		s.cmd.Log().WithError(err).Info("failed read from pipe")
	}

	// Assume that the error is not retryable. Try to kill now so that we can
	// return a nice *ZFSError with captured stderr. If the kill doesn't work,
	// it doesn't matter because the caller must by contract call Close()
	// anyways.
	if err := s.closeWait(); err != nil {
		return n, err
	}
	return n, err
}

func (s *SendStream) Close() error {
	debug("sendStream: close called")
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.state == sendStreamClosed {
		return os.ErrClosed
	} else if s.state != sendStreamOpen {
		panic("unreachable")
	}
	return s.closeWait()
}

func (s *SendStream) closeWait() error {
	defer s.cancel()
	s.stdoutReader.Close()
	s.state = sendStreamClosed

	if err := s.wait(); err != nil {
		debug("sendStream: wait: err=%T %s", err, err)
		s.exitErr = NewZfsError(err, s.stderrBuf.Bytes())
		return s.exitErr
	}
	return nil
}

func (s *SendStream) wait() error {
	if s.testMode {
		return s.cmd.WaitPipe()
	}
	return s.cmd.Wait()
}

func (s *SendStream) TestOnly_ReplaceStdoutReader(f io.ReadCloser,
) io.ReadCloser {
	prev := s.stdoutReader
	s.stdoutReader = f
	return prev
}

func (s *SendStream) TestOnly_ExitErr() *ZFSError { return s.exitErr }

// NOTE: When updating this struct, make sure to update funcs Validate ValidateCorrespondsToResumeToken
type ZFSSendArgVersion struct {
	RelName string
	GUID    uint64
}

func (v ZFSSendArgVersion) GetGuid() uint64                     { return v.GUID }
func (v ZFSSendArgVersion) ToSendArgVersion() ZFSSendArgVersion { return v }

func (v ZFSSendArgVersion) ValidateInMemory(fs string) error {
	if fs == "" {
		panic(fs)
	}
	if len(v.RelName) == 0 {
		return errors.New("`RelName` must not be empty")
	}

	var et EntityType
	switch v.RelName[0] {
	case '@':
		et = EntityTypeSnapshot
	case '#':
		et = EntityTypeBookmark
	default:
		return fmt.Errorf("`RelName` field must start with @ or #, got %q", v.RelName)
	}

	full := v.fullPathUnchecked(fs)
	if err := EntityNamecheck(full, et); err != nil {
		return err
	}

	return nil
}

func (v ZFSSendArgVersion) mustValidateInMemory(fs string) {
	if err := v.ValidateInMemory(fs); err != nil {
		panic(err)
	}
}

// fs must be not empty
func (a ZFSSendArgVersion) ValidateExistsAndGetVersion(ctx context.Context, fs string) (v FilesystemVersion, _ error) {
	if err := a.ValidateInMemory(fs); err != nil {
		return v, nil
	}

	realVersion, err := ZFSGetFilesystemVersion(ctx, a.FullPath(fs))
	if err != nil {
		return v, err
	}

	if realVersion.Guid != a.GUID {
		return v, fmt.Errorf("`GUID` field does not match real dataset's GUID: %q != %q", realVersion.Guid, a.GUID)
	}

	return realVersion, nil
}

func (a ZFSSendArgVersion) ValidateExists(ctx context.Context, fs string) error {
	_, err := a.ValidateExistsAndGetVersion(ctx, fs)
	return err
}

func (v ZFSSendArgVersion) FullPath(fs string) string {
	v.mustValidateInMemory(fs)
	return v.fullPathUnchecked(fs)
}

func (v ZFSSendArgVersion) fullPathUnchecked(fs string) string {
	return fmt.Sprintf("%s%s", fs, v.RelName)
}

func (v ZFSSendArgVersion) IsSnapshot() bool {
	v.mustValidateInMemory("unimportant")
	return v.RelName[0] == '@'
}

func (v ZFSSendArgVersion) MustBeBookmark() {
	v.mustValidateInMemory("unimportant")
	if v.RelName[0] != '#' {
		panic(fmt.Sprintf("must be bookmark, got %q", v.RelName))
	}
}

// When updating this struct, check Validate and ValidateCorrespondsToResumeToken (POTENTIALLY SECURITY SENSITIVE)
type ZFSSendArgsUnvalidated struct {
	FS       string
	From, To *ZFSSendArgVersion // From may be nil
	ZFSSendFlags
}

type ZFSSendArgsValidated struct {
	ZFSSendArgsUnvalidated
	FromVersion *FilesystemVersion
	ToVersion   FilesystemVersion
}

type ZFSSendFlags struct {
	Encrypted        *nodefault.Bool
	Properties       bool
	BackupProperties bool
	Raw              bool
	LargeBlocks      bool
	Compressed       bool
	EmbeddedData     bool
	Saved            bool

	// Preferred if not empty
	ResumeToken string // if not nil, must match what is specified in From, To (covered by ValidateCorrespondsToResumeToken)
}

type zfsSendArgsValidationContext struct {
	encEnabled *nodefault.Bool
}

type ZFSSendArgsValidationErrorCode int

const (
	ZFSSendArgsGenericValidationError ZFSSendArgsValidationErrorCode = 1 + iota
	ZFSSendArgsEncryptedSendRequestedButFSUnencrypted
	ZFSSendArgsFSEncryptionCheckFail
	ZFSSendArgsResumeTokenMismatch
)

type ZFSSendArgsValidationError struct {
	Args ZFSSendArgsUnvalidated
	What ZFSSendArgsValidationErrorCode
	Msg  error
}

func newValidationError(sendArgs ZFSSendArgsUnvalidated, what ZFSSendArgsValidationErrorCode, cause error) *ZFSSendArgsValidationError {
	return &ZFSSendArgsValidationError{sendArgs, what, cause}
}

func newGenericValidationError(sendArgs ZFSSendArgsUnvalidated, cause error) *ZFSSendArgsValidationError {
	return &ZFSSendArgsValidationError{sendArgs, ZFSSendArgsGenericValidationError, cause}
}

func (e ZFSSendArgsValidationError) Error() string {
	return e.Msg.Error()
}

type zfsSendArgsSkipValidationKeyType struct{}

var zfsSendArgsSkipValidationKey = zfsSendArgsSkipValidationKeyType{}

func ZFSSendArgsSkipValidation(ctx context.Context) context.Context {
	return context.WithValue(ctx, zfsSendArgsSkipValidationKey, true)
}

// - Recursively call Validate on each field.
// - Make sure that if ResumeToken != "", it reflects the same operation as the other parameters would.
//
// This function is not pure because GUIDs are checked against the local host's datasets.
func (a ZFSSendArgsUnvalidated) Validate(ctx context.Context) (v ZFSSendArgsValidated, _ error) {
	if dp, err := NewDatasetPath(a.FS); err != nil || dp.Length() == 0 {
		return v, newGenericValidationError(a, errors.New("`FS` must be a valid non-zero dataset path"))
	}

	if a.To == nil {
		return v, newGenericValidationError(a, errors.New("`To` must not be nil"))
	}
	toVersion, err := a.To.ValidateExistsAndGetVersion(ctx, a.FS)
	if err != nil {
		return v, newGenericValidationError(a, fmt.Errorf("`To` invalid: %w", err))
	}

	var fromVersion *FilesystemVersion
	if a.From != nil {
		fromV, err := a.From.ValidateExistsAndGetVersion(ctx, a.FS)
		if err != nil {
			return v, newGenericValidationError(a, fmt.Errorf("`From` invalid: %w", err))
		}
		fromVersion = &fromV
		// fallthrough
	}

	validated := ZFSSendArgsValidated{
		ZFSSendArgsUnvalidated: a,
		FromVersion:            fromVersion,
		ToVersion:              toVersion,
	}

	if ctx.Value(zfsSendArgsSkipValidationKey) != nil {
		return validated, nil
	}

	if err := a.ZFSSendFlags.Validate(); err != nil {
		return v, newGenericValidationError(a, fmt.Errorf("send flags invalid: %w", err))
	}

	valCtx := &zfsSendArgsValidationContext{}
	fsEncrypted, err := ZFSGetEncryptionEnabled(ctx, a.FS)
	if err != nil {
		return v, newValidationError(a, ZFSSendArgsFSEncryptionCheckFail,
			fmt.Errorf("cannot check whether filesystem %q is encrypted: %w", a.FS, err))
	}
	valCtx.encEnabled = &nodefault.Bool{B: fsEncrypted}

	if a.Encrypted.B && !fsEncrypted {
		return v, newValidationError(a, ZFSSendArgsEncryptedSendRequestedButFSUnencrypted,
			fmt.Errorf("encrypted send mandated by policy, but filesystem %q is not encrypted", a.FS))
	}

	if a.Raw && fsEncrypted && !a.Encrypted.B {
		return v, newValidationError(a, ZFSSendArgsGenericValidationError,
			fmt.Errorf("policy mandates raw+unencrypted sends, but filesystem %q is encrypted", a.FS))
	}

	if err := a.validateEncryptionFlagsCorrespondToResumeToken(ctx, valCtx); err != nil {
		return v, newValidationError(a, ZFSSendArgsResumeTokenMismatch, err)
	}

	return validated, nil
}

func (f ZFSSendFlags) Validate() error {
	if err := f.Encrypted.ValidateNoDefault(); err != nil {
		return fmt.Errorf("flag `Encrypted` invalid: %w", err)
	}
	return nil
}

// If ResumeToken is empty, builds a command line with the flags specified.
// If ResumeToken is not empty, build a command line with just `-t {{.ResumeToken}}`.
//
// SECURITY SENSITIVE it is the caller's responsibility to ensure that a.Encrypted semantics
// hold for the file system that will be sent with the send flags returned by this function
func (a ZFSSendFlags) buildSendFlagsUnchecked() []string {
	args := make([]string, 0)

	// ResumeToken takes precedence, we assume that it has been validated
	// to reflect what is described by the other fields.
	if a.ResumeToken != "" {
		args = append(args, "-t", a.ResumeToken)
		return args
	}

	if a.Encrypted.B || a.Raw {
		args = append(args, "-w")
	}

	if a.Properties {
		args = append(args, "-p")
	}

	if a.BackupProperties {
		args = append(args, "-b")
	}

	if a.LargeBlocks {
		args = append(args, "-L")
	}

	if a.Compressed {
		args = append(args, "-c")
	}

	if a.EmbeddedData {
		args = append(args, "-e")
	}

	if a.Saved {
		args = append(args, "-S")
	}

	return args
}

func (a ZFSSendArgsValidated) buildSendCommandLine() ([]string, error) {
	flags := a.buildSendFlagsUnchecked()

	if a.ZFSSendFlags.ResumeToken != "" {
		return flags, nil
	}

	toV, err := absVersion(a.FS, a.To)
	if err != nil {
		return nil, err
	}

	fromV := ""
	if a.From != nil {
		fromV, err = absVersion(a.FS, a.From)
		if err != nil {
			return nil, err
		}
	}

	if fromV == "" { // Initial
		flags = append(flags, toV)
	} else if a.From.RelName[0] == '@' { // snapshot
		flags = append(flags, "-I", fromV, toV)
	} else {
		flags = append(flags, "-i", fromV, toV)
	}
	return flags, nil
}

type ZFSSendArgsResumeTokenMismatchError struct {
	What ZFSSendArgsResumeTokenMismatchErrorCode
	Err  error
}

func (e *ZFSSendArgsResumeTokenMismatchError) Error() string { return e.Err.Error() }

type ZFSSendArgsResumeTokenMismatchErrorCode int

// The format is ZFSSendArgsResumeTokenMismatch+WhatIsWrongInToken
const (
	ZFSSendArgsResumeTokenMismatchGeneric          ZFSSendArgsResumeTokenMismatchErrorCode = 1 + iota
	ZFSSendArgsResumeTokenMismatchEncryptionNotSet                                         // encryption not set in token but required by send args
	ZFSSendArgsResumeTokenMismatchEncryptionSet                                            // encryption not set in token but not required by send args
	ZFSSendArgsResumeTokenMismatchFilesystem
)

func (c ZFSSendArgsResumeTokenMismatchErrorCode) fmt(format string, args ...interface{}) *ZFSSendArgsResumeTokenMismatchError {
	return &ZFSSendArgsResumeTokenMismatchError{
		What: c,
		Err:  fmt.Errorf(format, args...),
	}
}

// Validate that the encryption settings specified in `a` correspond to the encryption settings encoded in the resume token.
//
// This is SECURITY SENSITIVE:
// It is possible for an attacker to craft arbitrary resume tokens.
// Those malicious resume tokens could encode different parameters in the resume token than expected:
// for example, they may specify another file system (e.g. the filesystem with secret data) or request unencrypted send instead of encrypted raw send.
//
// Note that we don't check correspondence of all other send flags because
// a) the resume token does not capture all send flags (e.g. send -p is implemented in libzfs and thus not represented in the resume token)
// b) it would force us to either reject resume tokens with unknown flags.
func (a ZFSSendArgsUnvalidated) validateEncryptionFlagsCorrespondToResumeToken(ctx context.Context, valCtx *zfsSendArgsValidationContext) error {
	if a.ResumeToken == "" {
		return nil // nothing to do
	}

	debug("decoding resume token %q", a.ResumeToken)
	t, err := ParseResumeToken(ctx, a.ResumeToken)
	debug("decode resume token result: %#v %T %v", t, err, err)
	if err != nil {
		return err
	}

	tokenFS, _, err := t.ToNameSplit()
	if err != nil {
		return err
	}

	gen := ZFSSendArgsResumeTokenMismatchGeneric

	if a.FS != tokenFS.ToString() {
		return ZFSSendArgsResumeTokenMismatchFilesystem.fmt(
			"filesystem in resume token field `toname` = %q does not match expected value %q", tokenFS.ToString(), a.FS)
	}

	// If From is set, it must match.
	if (a.From != nil) != t.HasFromGUID { // existence must be same
		if t.HasFromGUID {
			return gen.fmt("resume token not expected to be incremental, but `fromguid` = %v", t.FromGUID)
		} else {
			return gen.fmt("resume token expected to be incremental, but `fromguid` not present")
		}
	} else if t.HasFromGUID { // if exists (which is same, we checked above), they must match
		if t.FromGUID != a.From.GUID {
			return gen.fmt("resume token `fromguid` != expected: %v != %v", t.FromGUID, a.From.GUID)
		}
	} else {
		_ = struct{}{} // both empty, ok
	}

	// To must never be empty
	if !t.HasToGUID {
		return gen.fmt("resume token does not have `toguid`")
	}
	if t.ToGUID != a.To.GUID { // a.To != nil because Validate checks for that
		return gen.fmt("resume token `toguid` != expected: %v != %v", t.ToGUID, a.To.GUID)
	}

	// ensure resume stream will be encrypted/unencrypted as specified in policy
	if err := valCtx.encEnabled.ValidateNoDefault(); err != nil {
		panic(valCtx)
	}
	wouldSendEncryptedIfFilesystemIsEncrypted := t.RawOK
	filesystemIsEncrypted := valCtx.encEnabled.B
	resumeWillBeEncryptedSend := filesystemIsEncrypted && wouldSendEncryptedIfFilesystemIsEncrypted
	if a.Encrypted.B {
		if resumeWillBeEncryptedSend {
			return nil // encrypted send in policy, and that's what's going to happen
		} else {
			if !filesystemIsEncrypted {
				// NB: a.Encrypted.B && !valCtx.encEnabled.B
				//     is handled in the caller, because it doesn't concern the resume token (different kind of error)
				panic("caller should have already raised an error")
			}
			// XXX we have no test coverage for this case. We'd need to forge a resume token for that.
			return ZFSSendArgsResumeTokenMismatchEncryptionNotSet.fmt(
				"resume token does not have rawok=true which would result in an unencrypted send, but policy mandates encrypted sends only")
		}
	} else {
		if resumeWillBeEncryptedSend {
			return ZFSSendArgsResumeTokenMismatchEncryptionSet.fmt(
				"resume token has rawok=true which would result in encrypted send, but policy mandates unencrypted sends only")
		} else {
			return nil // unencrypted send in policy, and that's what's going to happen
		}
	}
}

var ErrEncryptedSendNotSupported = errors.New(
	"raw sends which are required for encrypted zfs send are not supported")

// if token != "", then send -t token is used
// otherwise send [-i from] to is used
// (if from is "" a full ZFS send is done)
//
// Returns ErrEncryptedSendNotSupported if encrypted send is requested but not
// supported by CLI.
func ZFSSend(
	ctx context.Context, sendArgs ZFSSendArgsValidated, pipeCmds ...[]string,
) (*SendStream, error) {
	// Pre-validation of sendArgs for plain ErrEncryptedSendNotSupported error. We
	// tie BackupProperties (send -b) and SendRaw (-w, same as with Encrypted) to
	// this since these were released together.
	if sendArgs.Encrypted.B {
		if encryptionSupported, err := EncryptionCLISupported(ctx); err != nil {
			return nil, fmt.Errorf("cannot determine CLI native encryption support: %w", err)
		} else if !encryptionSupported {
			return nil, ErrEncryptedSendNotSupported
		}
	}

	sargs, err := sendArgs.buildSendCommandLine()
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, len(sargs)+1)
	args = append(args, "send")
	args = append(args, sargs...)

	ctx, cancel := context.WithCancel(ctx)
	cmd := zfscmd.New(ctx).WithPipeLen(len(pipeCmds)).WithCommand(ZfsBin, args)
	stderrBuf := new(bytes.Buffer)
	pipeReader, err := cmd.PipeTo(pipeCmds, nil, stderrBuf)
	if err != nil {
		cancel()
		return nil, err
	} else if err := cmd.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("cannot start zfs send command: %w", err)
	}
	return NewSendStream(cmd, pipeReader, stderrBuf, cancel), nil
}

type DrySendType string

const (
	DrySendTypeFull        DrySendType = "full"
	DrySendTypeIncremental DrySendType = "incremental"
)

func DrySendTypeFromString(s string) (DrySendType, error) {
	switch s {
	case string(DrySendTypeFull):
		return DrySendTypeFull, nil
	case string(DrySendTypeIncremental):
		return DrySendTypeIncremental, nil
	default:
		return "", fmt.Errorf("unknown dry send type %q", s)
	}
}

type DrySendInfo struct {
	Type         DrySendType
	Filesystem   string // parsed from To field
	From, To     string // direct copy from ZFS output
	SizeEstimate uint64 // 0 if size estimate is not possible
}

// see test cases for example output
func (s *DrySendInfo) unmarshalZFSOutput(output []byte) error {
	debug("DrySendInfo.unmarshalZFSOutput: output=%q", output)
	scan := bufio.NewScanner(bytes.NewReader(output))
	for scan.Scan() {
		l := scan.Text()
		if err := s.unmarshalInfoLine(l); err != nil {
			return fmt.Errorf("line %q: %w", l, err)
		}
	}

	if s.Type != DrySendTypeFull && s.Type != DrySendTypeIncremental {
		return fmt.Errorf("no match for info line in:\n%s", output)
	}
	return nil
}

// unmarshal info line, looks like this:
//
//	full	zroot/test/a@1	5389768
//	incremental	zroot/test/a@1	zroot/test/a@2	5383936
//	size 5383936
//
// => see test cases
func (s *DrySendInfo) unmarshalInfoLine(l string) error {
	dryFields := strings.SplitN(l, "\t", 5)
	n := len(dryFields)
	if n == 0 {
		return nil
	}

	snapType := dryFields[0]
	var from, to, size string
	switch {
	case snapType == "full" && n > 1:
		to = dryFields[1]
		if n > 2 {
			size = dryFields[2]
		}
	case snapType == "incremental" && n > 2:
		from, to = dryFields[1], dryFields[2]
		if n > 3 {
			size = dryFields[3]
		}
		if s.From == "" {
			s.From = from
		}
	case snapType == "size" && n > 1:
		size = dryFields[1]
	default:
		return nil
	}

	if snapType == "full" || snapType == "incremental" {
		if sendType, err := DrySendTypeFromString(snapType); err != nil {
			return err
		} else if s.Type == "" {
			s.Type = sendType
		} else if sendType != s.Type {
			return fmt.Errorf("dry send type changed from %q to %q", s.Type, sendType)
		}
		s.To = to
		if s.Filesystem == "" {
			toFS, _, _, err := DecomposeVersionString(to)
			if err != nil {
				return fmt.Errorf("'to' is not a valid filesystem version: %s", err)
			}
			s.Filesystem = toFS
		}
		if size == "" {
			// workaround for OpenZFS 0.7 prior to
			//
			//   https://github.com/openzfs/zfs/commit/835db58592d7d947e5818eb7281882e2a46073e0#diff-66bd524398bcd2ac70d90925ab6d8073L1245
			//
			// see https://github.com/zrepl/zrepl/issues/289
			return nil
		}
	}

	if sizeEstimate, err := strconv.ParseUint(size, 10, 64); err != nil {
		return fmt.Errorf("cannot not parse size %q: %s", size, err)
	} else if snapType == "size" {
		s.SizeEstimate = max(sizeEstimate, s.SizeEstimate)
	} else {
		s.SizeEstimate += sizeEstimate
	}
	return nil
}

// to may be "", in which case a full ZFS send is done
// May return BookmarkSizeEstimationNotSupported as err if from is a bookmark.
func ZFSSendDry(ctx context.Context, sendArgs ZFSSendArgsValidated,
) (*DrySendInfo, error) {
	args := []string{"send", "-n", "-v", "-P"}
	sargs, err := sendArgs.buildSendCommandLine()
	if err != nil {
		return nil, err
	}
	args = append(args, sargs...)

	cmd := zfscmd.CommandContext(ctx, ZfsBin, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, NewZfsError(err, output)
	}

	si := new(DrySendInfo)
	if err := si.unmarshalZFSOutput(output); err != nil {
		return nil, fmt.Errorf("could not parse zfs send -n output: %s", err)
	}
	return si, nil
}

type ErrRecvResumeNotSupported struct {
	FS       string
	CheckErr error
}

func (e *ErrRecvResumeNotSupported) Error() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "zfs resumable recv into %q: ", e.FS)
	if e.CheckErr != nil {
		fmt.Fprint(&buf, e.CheckErr.Error())
	} else {
		fmt.Fprintf(&buf, "not supported by ZFS or pool")
	}
	return buf.String()
}

type RecvOptions struct {
	// Rollback to the oldest snapshot, destroy it, then perform `recv -F`. Note
	// that this doesn't change property values, i.e. an existing local property
	// value will be kept.
	RollbackAndForceRecv bool
	// Set -s flag used for resumable send & recv
	SavePartialRecvState bool

	InheritProperties  []zfsprop.Property
	OverrideProperties map[zfsprop.Property]string

	stdinWrapper func(r io.Reader) io.Reader
}

func (self *RecvOptions) buildRecvFlags() []string {
	args := make([]string, 0,
		2+len(self.InheritProperties)*2+len(self.OverrideProperties)*2)

	if self.RollbackAndForceRecv {
		args = append(args, "-F")
	}

	if self.SavePartialRecvState {
		args = append(args, "-s")
	}

	if len(self.InheritProperties) != 0 {
		for _, prop := range self.InheritProperties {
			args = append(args, "-x", string(prop))
		}
	}

	if len(self.OverrideProperties) != 0 {
		for prop, value := range self.OverrideProperties {
			args = append(args, "-o", string(prop)+"="+value)
		}
	}
	return args
}

func (self *RecvOptions) WithStdinWrapper(wrapper func(r io.Reader) io.Reader,
) *RecvOptions {
	self.stdinWrapper = wrapper
	return self
}

func ZFSRecv(
	ctx context.Context, fs string, v *ZFSSendArgVersion, stream io.ReadCloser,
	opts RecvOptions, pipeCmds ...[]string,
) error {
	defer stream.Close()
	if err := v.ValidateInMemory(fs); err != nil {
		return fmt.Errorf("invalid version: %w", err)
	} else if !v.IsSnapshot() {
		return errors.New("must receive into a snapshot")
	}

	fsdp, err := NewDatasetPath(fs)
	if err != nil {
		return err
	}

	if opts.RollbackAndForceRecv {
		// Destroy all snapshots before `recv -F` because `recv -F` does not perform
		// a rollback unless `send -R` was used (which we assume hasn't been the
		// case).
		if err := zfsRollbackForceRecv(ctx, fsdp); err != nil {
			return nil
		}
	}

	if opts.SavePartialRecvState {
		if supported, err := ResumeRecvSupported(ctx, fsdp); err != nil || !supported {
			return &ErrRecvResumeNotSupported{FS: fs, CheckErr: err}
		}
	}

	recvFlags := opts.buildRecvFlags()
	args := make([]string, 0, len(recvFlags)+2)
	args = append(args, "recv")
	args = append(args, recvFlags...)
	args = append(args, fs)
	cmd := zfscmd.New(ctx).WithPipeLen(len(pipeCmds)).
		WithCommand(ZfsBin, args)

	// TODO report bug upstream Setup an unused stdout buffer. Otherwise, ZoL
	// v0.6.5.9-1 3.16.0-4-amd64 writes the following error to stderr and exits
	// with code 1
	//
	//  cannot receive new filesystem stream: invalid backup stream
	var stderr bytes.Buffer

	if err := cmd.PipeFrom(pipeCmds, stream, &stderr, &stderr); err != nil {
		return err
	} else if opts.stdinWrapper != nil {
		cmd.WrapStdin(opts.stdinWrapper)
	}

	if err := cmd.Start(); err != nil {
		return err
	}

	pid := cmd.Process().Pid
	debug := func(format string, args ...any) {
		debug("recv: pid=%v: %s", pid, fmt.Sprintf(format, args...))
	}
	debug("started")

	if err := cmd.WithLogError(false).Wait(); err != nil {
		cmd.WithStderrOutput(stderr.Bytes()).LogError(err, false)
		err = parseZfsRecvErr(ctx, err, stderr.Bytes())
		debug("wait err: %T %s", err, err)
		// almost always more interesting info. NOTE: do not wrap!
		return err
	}
	return nil
}

func zfsRollbackForceRecv(ctx context.Context, fsdp *DatasetPath) error {
	snaps, err := ZFSListFilesystemVersions(ctx, fsdp,
		ListFilesystemVersionsOptions{Types: Snapshots})
	if _, ok := err.(*DatasetDoesNotExist); ok {
		snaps = []FilesystemVersion{}
	} else if err != nil {
		return fmt.Errorf(
			"cannot list versions for rollback for forced receive: %s", err)
	} else if len(snaps) == 0 {
		return nil
	}

	slices.SortFunc(snaps, func(a, b FilesystemVersion) int {
		return cmp.Compare(a.CreateTXG, b.CreateTXG)
	})

	// bookmarks are rolled back automatically
	//
	// Use rollback to efficiently destroy all but the earliest snapshot, then
	// destroy that earliest snapshot, afterwards, `recv -F` will work.
	rollbackTarget := snaps[0]
	rollbackTargetAbs := rollbackTarget.ToAbsPath(fsdp)
	debug("recv: rollback to %q", rollbackTargetAbs)
	if err := ZFSRollback(ctx, fsdp, rollbackTarget, "-r"); err != nil {
		return fmt.Errorf(
			"cannot rollback %s to %s for forced receive: %s",
			fsdp.ToString(), rollbackTarget, err)
	}

	debug("recv: destroy %q", rollbackTargetAbs)
	if err := ZFSDestroy(ctx, rollbackTargetAbs); err != nil {
		return fmt.Errorf(
			"cannot destroy %s for forced receive: %s",
			rollbackTargetAbs, err)
	}
	return nil
}

func parseZfsRecvErr(ctx context.Context, err error, stderr []byte) error {
	if err := tryRecvErrorWithResumeToken(ctx, string(stderr)); err != nil {
		return err
	} else if err := tryRecvDestroyOrOverwriteEncryptedErr(stderr); err != nil {
		return err
	} else if err := tryRecvCannotReadFromStreamErr(stderr); err != nil {
		return err
	}
	return NewZfsError(err, stderr)
}

var recvErrorResumeTokenRE = regexp.MustCompile(`A resuming stream can be generated on the sending system by running:\s+zfs send -t\s(\S+)`)

func tryRecvErrorWithResumeToken(ctx context.Context, stderr string,
) *RecvFailedWithResumeTokenErr {
	match := recvErrorResumeTokenRE.FindStringSubmatch(stderr)
	if len(match) == 0 {
		return nil
	}

	parsed, err := ParseResumeToken(ctx, match[1])
	if err != nil {
		return nil
	}
	return &RecvFailedWithResumeTokenErr{
		Msg:               stderr,
		ResumeTokenRaw:    match[1],
		ResumeTokenParsed: parsed,
	}
}

type RecvFailedWithResumeTokenErr struct {
	Msg               string
	ResumeTokenRaw    string
	ResumeTokenParsed *ResumeToken
}

func (self *RecvFailedWithResumeTokenErr) Error() string {
	return fmt.Sprintf(
		"receive failed, resume token available: %s\n%#v",
		self.ResumeTokenRaw, self.ResumeTokenParsed)
}

type RecvDestroyOrOverwriteEncryptedErr struct {
	Msg string
}

func (e *RecvDestroyOrOverwriteEncryptedErr) Error() string {
	return e.Msg
}

var recvDestroyOrOverwriteEncryptedErrRe = regexp.MustCompile(`^(cannot receive new filesystem stream: zfs receive -F cannot be used to destroy an encrypted filesystem or overwrite an unencrypted one with an encrypted one)`)

func tryRecvDestroyOrOverwriteEncryptedErr(stderr []byte,
) *RecvDestroyOrOverwriteEncryptedErr {
	debug("tryRecvDestroyOrOverwriteEncryptedErr: %v", stderr)
	m := recvDestroyOrOverwriteEncryptedErrRe.FindSubmatch(stderr)
	if m == nil {
		return nil
	}
	return &RecvDestroyOrOverwriteEncryptedErr{Msg: string(m[1])}
}

type RecvCannotReadFromStreamErr struct {
	Msg string
}

func (e *RecvCannotReadFromStreamErr) Error() string {
	return e.Msg
}

var reRecvCannotReadFromStreamErr = regexp.MustCompile(
	`^(cannot receive: failed to read from stream)$`)

func tryRecvCannotReadFromStreamErr(stderr []byte,
) *RecvCannotReadFromStreamErr {
	m := reRecvCannotReadFromStreamErr.FindSubmatch(stderr)
	if m == nil {
		return nil
	}
	return &RecvCannotReadFromStreamErr{Msg: string(m[1])}
}

type ClearResumeTokenError struct {
	ZFSOutput []byte
	CmdError  error
}

func (e ClearResumeTokenError) Error() string {
	return fmt.Sprintf("could not clear resume token: %q", string(e.ZFSOutput))
}

// always returns *ClearResumeTokenError
func ZFSRecvClearResumeToken(ctx context.Context, fs string) error {
	if err := validateZFSFilesystem(fs); err != nil {
		return err
	}

	cmd := zfscmd.CommandContext(ctx, ZfsBin, "recv", "-A", fs).
		WithLogError(false)
	o, err := cmd.CombinedOutput()
	if err != nil {
		if bytes.Contains(o, []byte("does not have any resumable receive state to abort")) {
			cmd.LogError(err, true)
			return nil
		}
		cmd.LogError(err, false)
		return &ClearResumeTokenError{o, err}
	}
	return nil
}

func NewPropertyValue(v string, src PropertySource) PropertyValue {
	return PropertyValue{Value: v, Source: src}
}

type PropertyValue struct {
	Value  string
	Source PropertySource
}

type ZFSProperties struct {
	m map[string]PropertyValue
}

func NewZFSProperties() *ZFSProperties {
	return &ZFSProperties{make(map[string]PropertyValue, 4)}
}

func (p *ZFSProperties) Valid(props []string) bool {
	return len(p.m) == len(props)
}

func (p *ZFSProperties) Get(key string) string {
	return p.m[key].Value
}

func (p *ZFSProperties) GetDetails(key string) PropertyValue {
	return p.m[key]
}

func (p *ZFSProperties) Add(propName string, propValue PropertyValue) bool {
	if _, ok := p.m[propName]; ok {
		return false
	}
	p.m[propName] = propValue
	return true
}

func zfsSet(ctx context.Context, path string, props map[string]string) error {
	args := make([]string, 0, len(props)+2)
	args = append(args, "set")

	for prop, val := range props {
		if strings.Contains(prop, "=") {
			return errors.New(
				"prop contains rune '=' which is the delimiter between property name and value")
		}
		args = append(args, fmt.Sprintf("%s=%s", prop, val))
	}
	args = append(args, path)

	cmd := zfscmd.CommandContext(ctx, ZfsBin, args...)
	stdio, err := cmd.CombinedOutput()
	if err != nil {
		return NewZfsError(err, stdio)
	}
	return nil
}

func ZFSSet(ctx context.Context, fs *DatasetPath, props map[string]string) error {
	return zfsSet(ctx, fs.ToString(), props)
}

func ZFSGet(ctx context.Context, fs *DatasetPath, props []string) (*ZFSProperties, error) {
	return zfsGet(ctx, fs.ToString(), props, SourceAny)
}

// The returned error includes requested filesystem and version as quoted strings in its error message
func ZFSGetGUID(ctx context.Context, fs string, version string) (g uint64, err error) {
	defer func(e *error) {
		if *e != nil {
			*e = fmt.Errorf("zfs get guid fs=%q version=%q: %s", fs, version, *e)
		}
	}(&err)
	if err := validateZFSFilesystem(fs); err != nil {
		return 0, err
	}
	if len(version) == 0 {
		return 0, errors.New("version must have non-zero length")
	}
	if strings.IndexAny(version[0:1], "@#") != 0 {
		return 0, errors.New("version does not start with @ or #")
	}
	path := fmt.Sprintf("%s%s", fs, version)
	props, err := zfsGet(ctx, path, []string{"guid"}, SourceAny) // always local
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(props.Get("guid"), 10, 64)
}

type GetMountpointOutput struct {
	Mounted    bool
	Mountpoint string
}

func ZFSGetMountpoint(ctx context.Context, fs string) (*GetMountpointOutput, error) {
	if err := EntityNamecheck(fs, EntityTypeFilesystem); err != nil {
		return nil, err
	}
	props, err := zfsGet(ctx, fs, []string{"mountpoint", "mounted"}, SourceAny)
	if err != nil {
		return nil, err
	}
	o := &GetMountpointOutput{}
	o.Mounted = props.Get("mounted") == "yes"
	o.Mountpoint = props.Get("mountpoint")
	if o.Mountpoint == "none" {
		o.Mountpoint = ""
	}
	if o.Mounted && o.Mountpoint == "" {
		panic("unexpected zfs get output")
	}
	return o, nil
}

func ZFSGetRawAnySource(ctx context.Context, path string, props []string) (*ZFSProperties, error) {
	return zfsGet(ctx, path, props, SourceAny)
}

// verified in platformtest
var zfsGetDatasetDoesNotExistRegexp = regexp.MustCompile(
	`^cannot open '([^)]+)': (dataset does not exist|no such pool or dataset)`)

type DatasetDoesNotExist struct {
	Path string
}

func (d *DatasetDoesNotExist) Error() string { return fmt.Sprintf("dataset %q does not exist", d.Path) }

func tryDatasetDoesNotExist(expectPath string, stderr []byte,
) *DatasetDoesNotExist {
	if sm := zfsGetDatasetDoesNotExistRegexp.FindSubmatch(stderr); sm != nil {
		if string(sm[1]) == expectPath {
			return &DatasetDoesNotExist{expectPath}
		}
	}
	return nil
}

//go:generate enumer -type=PropertySource -trimprefix=Source
type PropertySource uint32

const (
	SourceLocal PropertySource = 1 << iota
	SourceDefault
	SourceInherited
	SourceNone
	SourceTemporary
	SourceReceived

	SourceAny PropertySource = ^PropertySource(0)
)

var propertySourceParseLUT = map[string]PropertySource{
	"local":     SourceLocal,
	"default":   SourceDefault,
	"inherited": SourceInherited,
	"-":         SourceNone,
	"temporary": SourceTemporary,
	"received":  SourceReceived,
}

func parsePropertySource(s string) (PropertySource, error) {
	fields := strings.Fields(s)
	if len(fields) > 0 {
		v, ok := propertySourceParseLUT[fields[0]]
		if ok {
			return v, nil
		}
		// fallthrough
	}
	return 0, fmt.Errorf("unknown property source %q", s)
}

func (s PropertySource) zfsGetSourceFieldPrefixes() []string {
	prefixes := make([]string, 0, 7)
	if s&SourceLocal != 0 {
		prefixes = append(prefixes, "local")
	}
	if s&SourceDefault != 0 {
		prefixes = append(prefixes, "default")
	}
	if s&SourceInherited != 0 {
		prefixes = append(prefixes, "inherited")
	}
	if s&SourceNone != 0 {
		prefixes = append(prefixes, "-")
	}
	if s&SourceTemporary != 0 {
		prefixes = append(prefixes, "temporary")
	}
	if s&SourceReceived != 0 {
		prefixes = append(prefixes, "received")
	}
	if s == SourceAny {
		prefixes = append(prefixes, "")
	}
	return prefixes
}

func ZFSGetRecursive(ctx context.Context, path string, depth int,
	dstypes []string, props []string, allowedSources PropertySource,
) (map[string]*ZFSProperties, error) {
	cmd := zfscmd.CommandContext(ctx, ZfsBin,
		zfsGetArgs(path, depth, dstypes, props)...).WithLogError(false)
	var stderrBuf bytes.Buffer
	stdout, err := cmd.StdoutPipeWithErrorBuf(&stderrBuf)
	if err != nil {
		return nil, err
	} else if err := cmd.Start(); err != nil {
		return nil, err
	}

	propsByFS := map[string]*ZFSProperties{}
	allowedPrefixes := allowedSources.zfsGetSourceFieldPrefixes()

	err = scanCmdOutput(cmd, stdout, &stderrBuf,
		func(s string) (error, bool) {
			if err := parsePropsByFs(s, propsByFS, allowedPrefixes); err != nil {
				return err, false
			}
			return nil, true
		})
	if err != nil {
		return nil, maybeDatasetNotExists(cmd, path, err)
	}

	// validate we got expected output
	return validatePropsByFs(propsByFS, props)
}

func zfsGetArgs(path string, depth int, dstypes []string,
	props []string,
) []string {
	args := make([]string, 0, 10)
	args = append(args, "get", "-Hp", "-o", "name,property,value,source")

	if depth != 0 {
		args = append(args, "-r")
		if depth != -1 {
			args = append(args, "-d", strconv.Itoa(depth))
		}
	}

	if len(dstypes) > 0 {
		args = append(args, "-t", strings.Join(dstypes, ","))
	}
	return append(args, strings.Join(props, ","), path)
}

func parsePropsByFs(s string, propsByFs map[string]*ZFSProperties,
	prefixes []string,
) error {
	fields := strings.SplitN(s, "\t", 5)
	if len(fields) != 4 {
		return fmt.Errorf(
			"zfs get did not return name,property,value,source tuples: %q", s)
	}
	fs, prop, value, srcStr := fields[0], fields[1], fields[2], fields[3]

	for _, p := range prefixes {
		// prefix-match so that SourceAny (= "") works
		if !strings.HasPrefix(srcStr, p) {
			continue
		}
		source, err := parsePropertySource(srcStr)
		if err != nil {
			return fmt.Errorf("parse property source %q: %w", srcStr, err)
		}
		fsProps, ok := propsByFs[fs]
		if !ok {
			fsProps = NewZFSProperties()
			propsByFs[fs] = fsProps
		}
		if !fsProps.Add(prop, NewPropertyValue(value, source)) {
			return fmt.Errorf(
				"duplicate property %q for dataset %q", prop, fs)
		}
		break
	}
	return nil
}

func validatePropsByFs(propsByFS map[string]*ZFSProperties, props []string,
) (map[string]*ZFSProperties, error) {
	for fs, fsProps := range propsByFS {
		if !fsProps.Valid(props) {
			return nil, fmt.Errorf(
				"zfs get did not return all requested values for dataset %q", fs)
		}
	}
	return propsByFS, nil
}

func zfsGet(ctx context.Context, path string, props []string,
	allowedSources PropertySource,
) (*ZFSProperties, error) {
	propMap, err := ZFSGetRecursive(ctx, path, 0, nil, props, allowedSources)
	switch {
	case err != nil:
		return nil, err
	case len(propMap) == 0:
		// XXX callers expect to always get a result here. They will observe
		// props.Get("propname") == "". We should change .Get to return a tuple, or
		// an error, or whatever.
		return NewZFSProperties(), nil
	case len(propMap) != 1:
		return nil, errors.New(
			"zfs get unexpectedly returned properties for multiple datasets")
	}

	res, ok := propMap[path]
	if !ok {
		return nil, errors.New(
			"zfs get returned properties for a different dataset that requested")
	}
	return res, nil
}

type DestroySnapshotsError struct {
	RawLines      []string
	Filesystem    string
	Undestroyable []string // snapshot name only (filesystem@ stripped)
	Reason        []string
}

func (e *DestroySnapshotsError) Error() string {
	if len(e.Undestroyable) != len(e.Reason) {
		panic(fmt.Sprintf("%v != %v", len(e.Undestroyable), len(e.Reason)))
	}
	if len(e.Undestroyable) == 0 {
		panic(fmt.Sprintf("error must have one undestroyable snapshot, %q", e.Filesystem))
	}
	if len(e.Undestroyable) == 1 {
		return fmt.Sprintf("zfs destroy failed: %s@%s: %s", e.Filesystem, e.Undestroyable[0], e.Reason[0])
	}
	return strings.Join(e.RawLines, "\n")
}

var destroySnapshotsErrorRegexp = regexp.MustCompile(`^cannot destroy snapshot ([^@]+)@(.+): (.*)$`) // yes, datasets can contain `:`

var destroyOneOrMoreSnapshotsNoneExistedErrorRegexp = regexp.MustCompile(`^could not find any snapshots to destroy; check snapshot names.`)

var destroyBookmarkDoesNotExist = regexp.MustCompile(`^bookmark '([^']+)' does not exist`)

func tryParseDestroySnapshotsError(arg string, stderr []byte) *DestroySnapshotsError {
	argComps := strings.SplitN(arg, "@", 2)
	if len(argComps) != 2 {
		return nil
	}
	filesystem := argComps[0]

	lines := bufio.NewScanner(bytes.NewReader(stderr))
	undestroyable := []string{}
	reason := []string{}
	rawLines := []string{}
	for lines.Scan() {
		line := lines.Text()
		rawLines = append(rawLines, line)
		m := destroySnapshotsErrorRegexp.FindStringSubmatch(line)
		if m == nil {
			return nil // unexpected line => be conservative
		} else {
			if m[1] != filesystem {
				return nil // unexpected line => be conservative
			}
			undestroyable = append(undestroyable, m[2])
			reason = append(reason, m[3])
		}
	}
	if len(undestroyable) == 0 {
		return nil
	}

	return &DestroySnapshotsError{
		RawLines:      rawLines,
		Filesystem:    filesystem,
		Undestroyable: undestroyable,
		Reason:        reason,
	}
}

func ZFSDestroy(ctx context.Context, arg string) error {
	var dstype, filesystem string
	idx := strings.IndexAny(arg, "@#")
	if idx == -1 {
		dstype = "filesystem"
		filesystem = arg
	} else {
		switch arg[idx] {
		case '@':
			dstype = "snapshot"
		case '#':
			dstype = "bookmark"
		}
		filesystem = arg[:idx]
	}

	defer prometheus.NewTimer(
		prom.ZFSDestroyDuration.WithLabelValues(dstype, filesystem))

	cmd := zfscmd.CommandContext(ctx, ZfsBin, "destroy", arg)
	if stdio, err := cmd.CombinedOutput(); err != nil {
		if destroyOneOrMoreSnapshotsNoneExistedErrorRegexp.Match(stdio) {
			return &DatasetDoesNotExist{arg}
		} else if match := destroyBookmarkDoesNotExist.FindStringSubmatch(string(stdio)); match != nil && match[1] == arg {
			return &DatasetDoesNotExist{arg}
		} else if dsNotExistErr := tryDatasetDoesNotExist(filesystem, stdio); dsNotExistErr != nil {
			return dsNotExistErr
		} else if dserr := tryParseDestroySnapshotsError(arg, stdio); dserr != nil {
			return dserr
		}
		return NewZfsError(err, stdio)
	}
	return nil
}

func ZFSDestroyIdempotent(ctx context.Context, path string) error {
	err := ZFSDestroy(ctx, path)
	if _, ok := err.(*DatasetDoesNotExist); ok {
		return nil
	}
	return err
}

func ZFSSnapshot(ctx context.Context, fs *DatasetPath, name string,
	recursive bool,
) error {
	promTimer := prometheus.NewTimer(
		prom.ZFSSnapshotDuration.WithLabelValues(fs.ToString()))
	defer promTimer.ObserveDuration()

	snapname := fs.ToString() + "@" + name
	if err := EntityNamecheck(snapname, EntityTypeSnapshot); err != nil {
		return fmt.Errorf("zfs snapshot: %w", err)
	}

	cmd := zfscmd.CommandContext(ctx, ZfsBin, "snapshot", snapname)
	if stdio, err := cmd.CombinedOutput(); err != nil {
		return NewZfsError(err, stdio)
	}
	return nil
}

var zfsBookmarkExistsRegex = regexp.MustCompile("^cannot create bookmark '[^']+': bookmark exists")

type BookmarkExists struct {
	zfsMsg         string
	fs, bookmark   string
	bookmarkOrigin ZFSSendArgVersion
	bookGuid       uint64
}

func (e *BookmarkExists) Error() string {
	return fmt.Sprintf("bookmark %s (guid=%v) with #%s: bookmark #%s exists but has different guid (%v)",
		e.bookmarkOrigin.FullPath(e.fs), e.bookmarkOrigin.GUID, e.bookmark, e.bookmark, e.bookGuid,
	)
}

var ErrBookmarkCloningNotSupported = errors.New("bookmark cloning feature is not yet supported by ZFS")

// idempotently create bookmark of the given version v
//
// if `v` is a bookmark, returns ErrBookmarkCloningNotSupported unless a
// bookmark with the name `bookmark` exists and has the same idenitty
// (zfs.FilesystemVersionEqualIdentity)
//
// v must be validated by the caller
func ZFSBookmark(ctx context.Context, fs string, v FilesystemVersion,
	bookmark string,
) (FilesystemVersion, error) {
	bm := FilesystemVersion{
		Type:     Bookmark,
		Name:     bookmark,
		UserRefs: OptionUint64{Valid: false},
		// bookmarks have the same createtxg, guid and creation as their origin
		CreateTXG: v.CreateTXG,
		Guid:      v.Guid,
		Creation:  v.Creation,
	}

	promTimer := prometheus.NewTimer(
		prom.ZFSBookmarkDuration.WithLabelValues(fs))
	defer promTimer.ObserveDuration()

	bookmarkname := fs + "#" + bookmark
	if err := EntityNamecheck(bookmarkname, EntityTypeBookmark); err != nil {
		return bm, err
	}

	if v.IsBookmark() {
		existingBm, err := ZFSGetFilesystemVersion(ctx, bookmarkname)
		if err != nil {
			var notExistsErr *DatasetDoesNotExist
			if errors.As(err, &notExistsErr) {
				return bm, ErrBookmarkCloningNotSupported
			}
			return bm, fmt.Errorf(
				"bookmark: idempotency check for bookmark cloning: %w", err)
		}
		if FilesystemVersionEqualIdentity(bm, existingBm) {
			return existingBm, nil
		}
		// TODO This is work in progress:
		// https://github.com/zfsonlinux/zfs/pull/9571
		return bm, ErrBookmarkCloningNotSupported
	}

	snapname := v.FullPath(fs)
	if err := EntityNamecheck(snapname, EntityTypeSnapshot); err != nil {
		return bm, err
	}

	cmd := zfscmd.CommandContext(ctx, ZfsBin, "bookmark", snapname, bookmarkname)
	if stdio, err := cmd.CombinedOutput(); err != nil {
		ddne := tryDatasetDoesNotExist(snapname, stdio)
		switch {
		case ddne != nil:
			return bm, ddne
		case zfsBookmarkExistsRegex.Match(stdio):
			// check if this was idempotent
			bookGuid, err := ZFSGetGUID(ctx, fs, "#"+bookmark)
			switch {
			case err != nil:
				// guid error expressive enough
				return bm, fmt.Errorf(
					"bookmark: idempotency check for bookmark creation: %w", err)
			case v.Guid == bookGuid:
				debug("bookmark: %q %q was idempotent: {snap,book}guid %d == %d",
					snapname, bookmarkname, v.Guid, bookGuid)
				return bm, nil
			}
			return bm, &BookmarkExists{
				fs:             fs,
				bookmarkOrigin: v.ToSendArgVersion(),
				bookmark:       bookmark,
				zfsMsg:         string(stdio),
				bookGuid:       bookGuid,
			}
		}
		return bm, NewZfsError(err, stdio)
	}
	return bm, nil
}

func ZFSRollback(ctx context.Context, fs *DatasetPath,
	snapshot FilesystemVersion, rollbackArgs ...string,
) error {
	snapabs := snapshot.ToAbsPath(fs)
	if snapshot.Type != Snapshot {
		return fmt.Errorf("can only rollback to snapshots, got %s", snapabs)
	}

	args := make([]string, 0, len(rollbackArgs)+2)
	args = append(args, "rollback")
	args = append(args, rollbackArgs...)
	args = append(args, snapabs)

	cmd := zfscmd.CommandContext(ctx, ZfsBin, args...)
	if stdio, err := cmd.CombinedOutput(); err != nil {
		return NewZfsError(err, stdio)
	}
	return nil
}
