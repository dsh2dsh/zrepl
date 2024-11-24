// Package endpoint implements replication endpoints for use with package
// replication.
package endpoint

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"path"
	"runtime"
	"slices"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/dsh2dsh/zrepl/internal/logger"
	"github.com/dsh2dsh/zrepl/internal/replication/logic/pdu"
	"github.com/dsh2dsh/zrepl/internal/util/chainlock"
	"github.com/dsh2dsh/zrepl/internal/util/nodefault"
	"github.com/dsh2dsh/zrepl/internal/zfs"
	zfsprop "github.com/dsh2dsh/zrepl/internal/zfs/property"
)

type SenderConfig struct {
	FSF   zfs.DatasetFilter
	JobID JobID

	ListPlaceholders bool

	Encrypt              *nodefault.Bool
	SendRaw              bool
	SendProperties       bool
	SendBackupProperties bool
	SendLargeBlocks      bool
	SendCompressed       bool
	SendEmbeddedData     bool
	SendSaved            bool

	ExecPipe [][]string
}

func (c *SenderConfig) Validate() error {
	c.JobID.MustValidate()
	if err := c.Encrypt.ValidateNoDefault(); err != nil {
		return fmt.Errorf("`Encrypt` field invalid: %w", err)
	}
	if _, err := StepHoldTag(c.JobID); err != nil {
		return fmt.Errorf("JobID cannot be used for hold tag: %w", err)
	}
	return nil
}

// Sender implements replication.ReplicationEndpoint for a sending side
type Sender struct {
	FSFilter zfs.DatasetFilter
	jobId    JobID
	config   SenderConfig
}

func NewSender(conf SenderConfig) *Sender {
	if err := conf.Validate(); err != nil {
		panic("invalid config" + err.Error())
	}

	s := &Sender{
		FSFilter: conf.FSF,
		jobId:    conf.JobID,
		config:   conf,
	}
	return s
}

func (s *Sender) filterCheckFS(fs string) (*zfs.DatasetPath, error) {
	dp, err := zfs.NewDatasetPath(fs)
	if err != nil {
		return nil, err
	}
	if dp.Length() == 0 {
		return nil, errors.New("empty filesystem not allowed")
	}
	pass, err := s.FSFilter.Filter(dp)
	if err != nil {
		return nil, err
	}
	if !pass {
		return nil, fmt.Errorf("endpoint does not allow access to filesystem %s", fs)
	}
	return dp, nil
}

func (s *Sender) ListFilesystems(ctx context.Context) (*pdu.ListFilesystemRes,
	error,
) {
	if root := s.FSFilter.SingleRecursiveDataset(); root != nil {
		return listFilesystemsRecursive(ctx, root, true,
			zfs.PlaceholderPropertyName)
	}

	fss, err := zfs.ZFSListMapping(ctx, s.FSFilter)
	if err != nil {
		return nil, err
	}

	rfss := make([]*pdu.Filesystem, len(fss))
	for i, p := range fss {
		rfss[i] = &pdu.Filesystem{
			Path: p.ToString(),
			// ResumeToken does not make sense from Sender.
		}
	}

	if s.config.ListPlaceholders {
		if err := s.listPlaceholders(ctx, fss, rfss); err != nil {
			return nil, err
		}
	}
	res := &pdu.ListFilesystemRes{Filesystems: rfss}
	return res, nil
}

func (s *Sender) listPlaceholders(ctx context.Context, fss []*zfs.DatasetPath,
	rfss []*pdu.Filesystem,
) error {
	concurrency := runtime.GOMAXPROCS(0)
	getLogger(ctx).
		With(
			slog.Int("fs_count", len(fss)),
			slog.Int("concurrency", concurrency)).
		Info("get placeholder states")

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	begin := time.Now()
	for i, p := range fss {
		g.Go(func() error {
			l := getLogger(ctx).With(slog.String("fs", p.ToString()))
			ph, err := zfs.ZFSGetFilesystemPlaceholderState(ctx, p)
			if err != nil {
				logger.WithError(l, err, "error getting placeholder state")
				return fmt.Errorf(
					"cannot get placeholder state for fs %q: %w", p.ToString(), err)
			}
			l.With(slog.String("placeholder_state", fmt.Sprintf("%#v", ph))).
				Debug("placeholder state")
			if !ph.FSExists {
				l.Error("inconsistent placeholder state: filesystem must exists")
				return fmt.Errorf(
					"inconsistent placeholder state: filesystem %q must exist in this context",
					p.ToString())
			}
			rfss[i].IsPlaceholder = ph.IsPlaceholder
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err //nolint:wrapcheck // from our package
	}

	getLogger(ctx).With(slog.Duration("duration", time.Since(begin))).
		Info("got all placeholder states")
	return nil
}

func (s *Sender) ListFilesystemVersions(ctx context.Context, r *pdu.ListFilesystemVersionsReq) (*pdu.ListFilesystemVersionsRes, error) {
	lp, err := s.filterCheckFS(r.GetFilesystem())
	if err != nil {
		return nil, err
	}
	fsvs, err := zfs.ZFSListFilesystemVersions(ctx, lp, zfs.ListFilesystemVersionsOptions{})
	if err != nil {
		return nil, err
	}
	rfsvs := make([]*pdu.FilesystemVersion, len(fsvs))
	for i := range fsvs {
		rfsvs[i] = pdu.FilesystemVersionFromZFS(&fsvs[i])
	}
	res := &pdu.ListFilesystemVersionsRes{Versions: rfsvs}
	return res, nil
}

func uncheckedSendArgsFromPDU(fsv *pdu.FilesystemVersion) *zfs.ZFSSendArgVersion {
	if fsv == nil {
		return nil
	}
	return &zfs.ZFSSendArgVersion{RelName: fsv.GetRelName(), GUID: fsv.Guid}
}

func sendArgsFromPDUAndValidateExistsAndGetVersion(ctx context.Context, fs string, fsv *pdu.FilesystemVersion) (v zfs.FilesystemVersion, err error) {
	sendArgs := uncheckedSendArgsFromPDU(fsv)
	if sendArgs == nil {
		return v, errors.New("must not be nil")
	}
	version, err := sendArgs.ValidateExistsAndGetVersion(ctx, fs)
	if err != nil {
		return v, err
	}
	return version, nil
}

func (s *Sender) sendMakeArgs(ctx context.Context, r *pdu.SendReq) (sendArgs zfs.ZFSSendArgsValidated, _ error) {
	_, err := s.filterCheckFS(r.Filesystem)
	if err != nil {
		return sendArgs, err
	}

	sendArgsUnvalidated := zfs.ZFSSendArgsUnvalidated{
		FS:   r.Filesystem,
		From: uncheckedSendArgsFromPDU(r.GetFrom()), // validated by zfs.ZFSSendDry / zfs.ZFSSend
		To:   uncheckedSendArgsFromPDU(r.GetTo()),   // validated by zfs.ZFSSendDry / zfs.ZFSSend
		ZFSSendFlags: zfs.ZFSSendFlags{
			ResumeToken:      r.ResumeToken, // nil or not nil, depending on decoding success
			Encrypted:        s.config.Encrypt,
			Properties:       s.config.SendProperties,
			BackupProperties: s.config.SendBackupProperties,
			Raw:              s.config.SendRaw,
			LargeBlocks:      s.config.SendLargeBlocks,
			Compressed:       s.config.SendCompressed,
			EmbeddedData:     s.config.SendEmbeddedData,
			Saved:            s.config.SendSaved,
		},
	}

	sendArgs, err = sendArgsUnvalidated.Validate(ctx)
	if err != nil {
		return sendArgs, fmt.Errorf("validate send arguments: %w", err)
	}
	return sendArgs, nil
}

func (s *Sender) Send(ctx context.Context, r *pdu.SendReq) (*pdu.SendRes,
	io.ReadCloser, error,
) {
	sendArgs, err := s.sendMakeArgs(ctx, r)
	if err != nil {
		return nil, nil, err
	}

	// create holds or bookmarks of `From` and `To` to guarantee one of the
	// following:
	//
	// - that the replication step can always be resumed (`holds`),
	//
	// - that the replication step can be interrupted and a future replication
	//   step with same or different `To` but same `From` is still possible
	//   (`bookmarks`)
	//
	// - nothing (`none`)
	//
	// ...
	//
	// ... actually create the abstractions
	replicationGuaranteeOptions, err := replicationGuaranteeOptionsFromPDU(
		r.GetReplicationConfig().Protection)
	if err != nil {
		return nil, nil, err
	}
	replicationGuaranteeStrategy := replicationGuaranteeOptions.
		Strategy(sendArgs.From != nil)
	liveAbs, err := replicationGuaranteeStrategy.
		SenderPreSend(ctx, s.jobId, &sendArgs)
	if err != nil {
		return nil, nil, err
	}
	for _, a := range liveAbs {
		if a != nil {
			abstractionsCacheSingleton.Put(a)
		}
	}

	// cleanup the mess that _this function_ might have created in prior failed
	// attempts:
	//
	// In summary, we delete every endpoint ZFS abstraction created on this
	// filesystem for this job id, except for the ones we just created above.
	//
	// This is the most robust approach to avoid leaking (= forgetting to clean
	// up) endpoint ZFS abstractions, all under the assumption that there will
	// only ever be one send for a (jobId,fs) combination at any given time.
	//
	// Note that the SendCompleted rpc can't be relied upon for this purpose:
	//
	// - it might be lost due to network errors,
	//
	// - or never be sent by a potentially malicious or buggy client,
	//
	// - or never be send because the replication step failed at some point
	//   (potentially leaving a resumable state on the receiver, which is the case
	//   where we really do not want to blow away the step holds too soon.)
	//
	// Note further that a resuming send, due to the idempotent nature of func
	// CreateReplicationCursor and HoldStep, will never lose its step holds
	// because we just (idempotently re-)created them above, before attempting the
	// cleanup.
	destroyTypes := AbstractionTypeSet{
		AbstractionStepHold:                           true,
		AbstractionTentativeReplicationCursorBookmark: true,
	}
	// The replication planner can also pick an endpoint zfs abstraction as
	// FromVersion. Keep it, so that the replication will succeed.
	//
	// NB: there is no abstraction for snapshots, so, we only need to check
	// bookmarks.
	if sendArgs.FromVersion != nil && sendArgs.FromVersion.IsBookmark() {
		dp, err := zfs.NewDatasetPath(sendArgs.FS)
		if err != nil {
			panic(err) // sendArgs is validated, this shouldn't happen
		}
		liveAbs = append(liveAbs,
			destroyTypes.ExtractBookmark(dp, sendArgs.FromVersion))
	}
	func() {
		keep := func(a Abstraction) (keep bool) {
			keep = false
			for _, k := range liveAbs {
				keep = keep || AbstractionEquals(a, k)
			}
			return keep
		}
		check := func(obsoleteAbs []Abstraction) {
			// Ensure that we don't delete `From` or `To`. Regardless of whether they
			// are in AbstractionTypeSet or not. And produce a nice error message in
			// case we do, to aid debugging the resulting panic.
			//
			// This is especially important for `From`. We could break incremental
			// replication if we deleted the last common filesystem version between
			// sender and receiver.
			type Problem struct {
				sendArgsWhat string
				fullpath     string
				obsoleteAbs  Abstraction
			}
			problems := make([]Problem, 0)
			checkFullpaths := make(map[string]string, 2)
			checkFullpaths["ToVersion"] = sendArgs.ToVersion.
				FullPath(sendArgs.FS)
			if sendArgs.FromVersion != nil {
				checkFullpaths["FromVersion"] = sendArgs.FromVersion.
					FullPath(sendArgs.FS)
			}
			for _, a := range obsoleteAbs {
				for what, fullpath := range checkFullpaths {
					if a.GetFullPath() == fullpath && a.GetType().IsSnapshotOrBookmark() {
						problems = append(problems, Problem{
							sendArgsWhat: what,
							fullpath:     fullpath,
							obsoleteAbs:  a,
						})
					}
				}
			}
			if len(problems) == 0 {
				return
			}
			var msg strings.Builder
			fmt.Fprintf(&msg, "cleaning up send stale would destroy send args:\n")
			fmt.Fprintf(&msg, "  SendArgs: %#v\n", sendArgs)
			for _, check := range problems {
				fmt.Fprintf(&msg,
					"would delete %s %s because it was deemed an obsolete abstraction: %s\n",
					check.sendArgsWhat, check.fullpath, check.obsoleteAbs)
			}
			panic(msg.String())
		}
		abstractionsCacheSingleton.TryBatchDestroy(ctx,
			s.jobId, sendArgs.FS, destroyTypes, keep, check)
	}()

	var sendStream io.ReadCloser
	sendStream, err = zfs.ZFSSend(ctx, sendArgs, s.config.ExecPipe...)
	if err != nil {
		// it's ok to not destroy the abstractions we just created here, a new send
		// attempt will take care of it
		return nil, nil, fmt.Errorf("zfs send failed: %w", err)
	}

	res := &pdu.SendRes{UsedResumeToken: r.ResumeToken != ""}
	return res, sendStream, nil
}

func (s *Sender) SendDry(ctx context.Context, req *pdu.SendDryReq,
) (*pdu.SendDryRes, error) {
	if len(req.Items) == 0 {
		return &pdu.SendDryRes{}, nil
	}

	sendArgs, err := s.makeSendArgsList(ctx, req.Items)
	if err != nil {
		return nil, err
	} else if len(req.Items) == 1 {
		item, err := s.zfsSendDry(ctx, &req.Items[0], sendArgs[0])
		if err != nil {
			return nil, err
		}
		return &pdu.SendDryRes{Items: []pdu.SendRes{item}}, nil
	}

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(min(len(req.Items), req.Concurrency))

	resp := &pdu.SendDryRes{Items: make([]pdu.SendRes, len(req.Items))}
	for i := range req.Items {
		if ctx.Err() != nil {
			break
		}
		g.Go(func() (err error) {
			resp.Items[i], err = s.zfsSendDry(ctx, &req.Items[i], sendArgs[i])
			return
		})
	}
	return resp, g.Wait() //nolint:wrapcheck // it's our error
}

func (s *Sender) makeSendArgsList(ctx context.Context, items []pdu.SendReq,
) ([]zfs.ZFSSendArgsValidated, error) {
	sendArgs := make([]zfs.ZFSSendArgsValidated, len(items))
	for i := range items {
		args, err := s.sendMakeArgs(ctx, &items[i])
		if err != nil {
			return nil, err
		}
		sendArgs[i] = args
	}
	return sendArgs, nil
}

func (s *Sender) zfsSendDry(ctx context.Context, r *pdu.SendReq,
	args zfs.ZFSSendArgsValidated,
) (resp pdu.SendRes, err error) {
	si, err := zfs.ZFSSendDry(ctx, args)
	if err != nil {
		err = fmt.Errorf("zfs send dry failed: %w", err)
		return
	}
	resp = pdu.SendRes{
		ExpectedSize:    si.SizeEstimate,
		UsedResumeToken: r.ResumeToken != "",
	}
	return
}

func (p *Sender) SendCompleted(ctx context.Context, r *pdu.SendCompletedReq,
) error {
	orig := r.GetOriginalReq() // may be nil, always use proto getters
	fsp, err := p.filterCheckFS(orig.GetFilesystem())
	if err != nil {
		return err
	}
	fs := fsp.ToString()

	var from *zfs.FilesystemVersion
	if orig.GetFrom() != nil {
		f, err := sendArgsFromPDUAndValidateExistsAndGetVersion(ctx, fs, orig.GetFrom()) // no shadow
		if err != nil {
			return fmt.Errorf("validate `from` exists: %w", err)
		}
		from = &f
	}
	to, err := sendArgsFromPDUAndValidateExistsAndGetVersion(ctx, fs, orig.GetTo())
	if err != nil {
		return fmt.Errorf("validate `to` exists: %w", err)
	}

	replicationGuaranteeOptions, err := replicationGuaranteeOptionsFromPDU(orig.GetReplicationConfig().Protection)
	if err != nil {
		return err
	}
	liveAbs, err := replicationGuaranteeOptions.Strategy(from != nil).SenderPostRecvConfirmed(ctx, p.jobId, fs, to)
	if err != nil {
		return err
	}
	for _, a := range liveAbs {
		if a != nil {
			abstractionsCacheSingleton.Put(a)
		}
	}
	keep := func(a Abstraction) (keep bool) {
		keep = false
		for _, k := range liveAbs {
			keep = keep || AbstractionEquals(a, k)
		}
		return keep
	}
	destroyTypes := AbstractionTypeSet{
		AbstractionStepHold:                           true,
		AbstractionTentativeReplicationCursorBookmark: true,
		AbstractionReplicationCursorBookmarkV2:        true,
	}
	abstractionsCacheSingleton.TryBatchDestroy(ctx, p.jobId, fs, destroyTypes, keep, nil)

	return nil
}

func (p *Sender) DestroySnapshots(ctx context.Context, req *pdu.DestroySnapshotsReq) (*pdu.DestroySnapshotsRes, error) {
	dp, err := p.filterCheckFS(req.Filesystem)
	if err != nil {
		return nil, err
	}
	return doDestroySnapshots(ctx, dp, req.Snapshots)
}

func (p *Sender) WaitForConnectivity(ctx context.Context) error {
	return nil
}

func (p *Sender) ReplicationCursor(ctx context.Context, req *pdu.ReplicationCursorReq) (*pdu.ReplicationCursorRes, error) {
	dp, err := p.filterCheckFS(req.Filesystem)
	if err != nil {
		return nil, err
	}

	cursor, err := GetMostRecentReplicationCursorOfJob(ctx, dp.ToString(), p.jobId)
	if err != nil {
		return nil, err
	}
	if cursor == nil {
		return &pdu.ReplicationCursorRes{Result: &pdu.ReplicationCursorRes_Result{Notexist: true}}, nil
	}
	return &pdu.ReplicationCursorRes{Result: &pdu.ReplicationCursorRes_Result{Guid: cursor.Guid}}, nil
}

func (p *Sender) Receive(ctx context.Context, r *pdu.ReceiveReq,
	_ io.ReadCloser,
) error {
	return errors.New("sender does not implement Receive()")
}

type FSFilter interface { // FIXME unused
	Empty() bool
	Filter(path *zfs.DatasetPath) (pass bool, err error)
	UserSpecifiedDatasets() zfs.UserSpecifiedDatasetsSet
}

// FIXME: can we get away without error types here?
type FSMap interface { // FIXME unused
	FSFilter
	Map(path *zfs.DatasetPath) (*zfs.DatasetPath, error)
	Invert() (FSMap, error)
	AsFilter() FSFilter
}

// NOTE: when adding members to this struct, remember
// to add them to `ReceiverConfig.copyIn()`
type ReceiverConfig struct {
	JobID JobID

	RootWithoutClientComponent *zfs.DatasetPath
	AppendClientIdentity       bool

	InheritProperties  []zfsprop.Property
	OverrideProperties map[zfsprop.Property]string

	PlaceholderEncryption PlaceholderCreationEncryptionProperty

	ExecPipe [][]string
}

//go:generate enumer -type=PlaceholderCreationEncryptionProperty -transform=kebab -trimprefix=PlaceholderCreationEncryptionProperty
type PlaceholderCreationEncryptionProperty int

// Note: the constant names, transformed through enumer, are part of the config format!
const (
	PlaceholderCreationEncryptionPropertyUnspecified PlaceholderCreationEncryptionProperty = 1 << iota
	PlaceholderCreationEncryptionPropertyInherit
	PlaceholderCreationEncryptionPropertyOff
)

func (c *ReceiverConfig) copyIn() {
	c.RootWithoutClientComponent = c.RootWithoutClientComponent.Copy()

	pInherit := make([]zfsprop.Property, len(c.InheritProperties))
	copy(pInherit, c.InheritProperties)
	c.InheritProperties = pInherit

	pOverride := make(map[zfsprop.Property]string, len(c.OverrideProperties))
	for key, value := range c.OverrideProperties {
		pOverride[key] = value
	}
	c.OverrideProperties = pOverride
}

func (c *ReceiverConfig) Validate() error {
	c.JobID.MustValidate()

	for _, prop := range c.InheritProperties {
		err := prop.Validate()
		if err != nil {
			return fmt.Errorf("inherit property %q: %w", prop, err)
		}
	}

	for prop := range c.OverrideProperties {
		err := prop.Validate()
		if err != nil {
			return fmt.Errorf("override property %q: %w", prop, err)
		}
	}

	if c.RootWithoutClientComponent.Length() <= 0 {
		return errors.New("RootWithoutClientComponent must not be an empty dataset path")
	}

	if !c.PlaceholderEncryption.IsAPlaceholderCreationEncryptionProperty() {
		return errors.New("`PlaceholderEncryption` field is invalid")
	}

	return nil
}

func NewReceiver(config ReceiverConfig) *Receiver {
	config.copyIn()
	if err := config.Validate(); err != nil {
		panic(err)
	}

	r := &Receiver{
		conf:                  config,
		recvParentCreationMtx: chainlock.New(),
	}
	return r
}

// Receiver implements replication.ReplicationEndpoint for a receiving side
type Receiver struct {
	conf                  ReceiverConfig // validated
	recvParentCreationMtx *chainlock.L
	clientIdentity        string

	Test_OverrideClientIdentityFunc func() string // for use by platformtest
}

func (s *Receiver) WithClientIdentity(identity string) *Receiver {
	s.clientIdentity = identity
	return s
}

func (s *Receiver) clientRootFromCtx(ctx context.Context) *zfs.DatasetPath {
	if !s.conf.AppendClientIdentity {
		return s.conf.RootWithoutClientComponent.Copy()
	}

	var clientIdentity string
	switch {
	case s.clientIdentity != "":
		clientIdentity = s.clientIdentity
	case s.Test_OverrideClientIdentityFunc != nil:
		clientIdentity = s.Test_OverrideClientIdentityFunc()
	default:
		identity, ok := ctx.Value(ClientIdentityKey).(string) // no shadow
		if !ok {
			panic("ClientIdentityKey context value must be set")
		}
		clientIdentity = identity
	}

	clientRoot, err := clientRoot(s.conf.RootWithoutClientComponent,
		clientIdentity)
	if err != nil {
		err = fmt.Errorf(
			"ClientIdentityContextKey must have been validated before invoking Receiver: %w",
			err)
		panic(err)
	}
	return clientRoot
}

func clientRoot(rootFS *zfs.DatasetPath, clientIdentity string) (*zfs.DatasetPath, error) {
	rootFSLen := rootFS.Length()
	clientRootStr := path.Join(rootFS.ToString(), clientIdentity)
	clientRoot, err := zfs.NewDatasetPath(clientRootStr)
	if err != nil {
		return nil, err
	}
	if rootFSLen+1 != clientRoot.Length() {
		return nil, errors.New("client identity must be a single ZFS filesystem path component")
	}
	return clientRoot, nil
}

const receiveResumeToken = "receive_resume_token"

func (s *Receiver) ListFilesystems(ctx context.Context) (*pdu.ListFilesystemRes,
	error,
) {
	return listFilesystemsRecursive(ctx, s.clientRootFromCtx(ctx), false,
		zfs.PlaceholderPropertyName, receiveResumeToken)
}

func listFilesystemsRecursive(ctx context.Context, root *zfs.DatasetPath,
	includingRoot bool, props ...string,
) (*pdu.ListFilesystemRes, error) {
	rootStr := root.ToString()
	fsProps, err := zfs.ZFSGetRecursive(ctx, rootStr, -1,
		[]string{"filesystem"}, props, zfs.SourceAny)
	if err != nil {
		var errNotExist *zfs.DatasetDoesNotExist
		if errors.As(err, &errNotExist) {
			getLogger(ctx).With(slog.String("root", rootStr)).
				Debug("no filesystems found")
			return &pdu.ListFilesystemRes{}, nil
		}
		return nil, fmt.Errorf(
			"failed get properties of fs %q: %w", rootStr, err)
	}

	if !includingRoot {
		delete(fsProps, rootStr)
	}

	sortedProps := slices.SortedFunc(maps.Values(fsProps),
		func(a, b *zfs.ZFSProperties) int {
			return cmp.Compare(a.Order(), b.Order())
		})

	fss, err := makeFilesystems(ctx, root, includingRoot, sortedProps)
	if err != nil {
		return nil, err
	} else if len(fss) == 0 {
		getLogger(ctx).Debug("no filesystems found")
		return &pdu.ListFilesystemRes{}, nil
	}
	return &pdu.ListFilesystemRes{Filesystems: fss}, nil
}

func makeFilesystems(ctx context.Context, root *zfs.DatasetPath,
	includingRoot bool, items []*zfs.ZFSProperties,
) ([]*pdu.Filesystem, error) {
	// present filesystem without the root_fs prefix
	fss := make([]*pdu.Filesystem, 0, len(items))
	for _, props := range items {
		l := getLogger(ctx).WithField("fs", props.Fs())
		p, err := props.DatasetPath()
		if err != nil {
			l.WithError(err).Error("error getting placeholder state")
			return nil, fmt.Errorf(
				"cannot get placeholder state for fs %q: %w", props.Fs(), err)
		}

		state := zfs.NewPlaceholderState(p, props)
		l.WithField("placeholder_state", fmt.Sprintf("%#v", state)).
			Debug("placeholder state")
		if !state.FSExists {
			l.Error("inconsistent placeholder state: filesystem must exists")
			return nil, fmt.Errorf(
				"inconsistent placeholder state: filesystem %q must exist in this context",
				p.ToString())
		}

		tokenProp := props.GetDetails(receiveResumeToken)
		token := tokenProp.Value
		if tokenProp.Source != zfs.SourceLocal {
			token = ""
		}
		l.WithField("receive_resume_token", token).Debug("receive resume token")

		if !includingRoot {
			p.TrimPrefix(root)
		}

		fss = append(fss, &pdu.Filesystem{
			Path:          p.ToString(),
			IsPlaceholder: state.IsPlaceholder,
			ResumeToken:   token,
		})
	}
	return fss, nil
}

func (s *Receiver) ListFilesystemVersions(ctx context.Context,
	req *pdu.ListFilesystemVersionsReq,
) (*pdu.ListFilesystemVersionsRes, error) {
	lp, err := mapToLocal(s.clientRootFromCtx(ctx), req.GetFilesystem())
	if err != nil {
		return nil, err
	}
	// TODO share following code with sender

	fsvs, err := zfs.ZFSListFilesystemVersions(ctx, lp, zfs.ListFilesystemVersionsOptions{})
	if err != nil {
		return nil, err
	}

	rfsvs := make([]*pdu.FilesystemVersion, len(fsvs))
	for i := range fsvs {
		rfsvs[i] = pdu.FilesystemVersionFromZFS(&fsvs[i])
	}
	return &pdu.ListFilesystemVersionsRes{Versions: rfsvs}, nil
}

func mapToLocal(root *zfs.DatasetPath, fs string) (*zfs.DatasetPath, error) {
	p, err := zfs.NewDatasetPath(fs)
	if err != nil {
		return nil, err
	}
	if p.Length() == 0 {
		return nil, errors.New("cannot map empty filesystem")
	}
	c := root.Copy()
	c.Extend(p)
	return c, nil
}

func (s *Receiver) WaitForConnectivity(ctx context.Context) error {
	return nil
}

func (s *Receiver) ReplicationCursor(ctx context.Context, _ *pdu.ReplicationCursorReq) (*pdu.ReplicationCursorRes, error) {
	return nil, errors.New("ReplicationCursor not implemented for Receiver")
}

func (s *Receiver) Send(ctx context.Context, req *pdu.SendReq) (*pdu.SendRes, io.ReadCloser, error) {
	return nil, nil, errors.New("receiver does not implement Send()")
}

func (s *Receiver) SendDry(ctx context.Context, req *pdu.SendDryReq,
) (*pdu.SendDryRes, error) {
	return nil, errors.New("receiver does not implement SendDry()")
}

func (s *Receiver) receive_GetPlaceholderCreationEncryptionValue(client_root, path *zfs.DatasetPath) (zfs.FilesystemPlaceholderCreateEncryptionValue, error) {
	if !s.conf.PlaceholderEncryption.IsAPlaceholderCreationEncryptionProperty() {
		panic(s.conf.PlaceholderEncryption)
	}

	if client_root.Equal(path) && s.conf.PlaceholderEncryption == PlaceholderCreationEncryptionPropertyUnspecified {
		// If our Receiver is configured to append a client component to s.conf.RootWithoutClientComponent
		// then that dataset is always going to be a placeholder.
		// We don't want to burden users with the concept of placeholders if their `filesystems` filter on the sender
		// doesn't introduce any gaps.
		// Since the dataset hierarchy up to and including that client component dataset is still fully controlled by us,
		// using `inherit` is going to make it work in all expected use cases.
		return zfs.FilesystemPlaceholderCreateEncryptionInherit, nil
	}

	switch s.conf.PlaceholderEncryption {
	case PlaceholderCreationEncryptionPropertyUnspecified:
		return 0, errors.New("placeholder filesystem encryption handling is unspecified in receiver config")
	case PlaceholderCreationEncryptionPropertyInherit:
		return zfs.FilesystemPlaceholderCreateEncryptionInherit, nil
	case PlaceholderCreationEncryptionPropertyOff:
		return zfs.FilesystemPlaceholderCreateEncryptionOff, nil
	default:
		panic(s.conf.PlaceholderEncryption)
	}
}

func (s *Receiver) Receive(ctx context.Context, req *pdu.ReceiveReq,
	receive io.ReadCloser,
) error {
	defer receive.Close()
	getLogger(ctx).Debug("incoming Receive")

	root := s.clientRootFromCtx(ctx)
	lp, err := mapToLocal(root, req.Filesystem)
	if err != nil {
		return fmt.Errorf("`Filesystem` invalid: %w", err)
	}

	to := uncheckedSendArgsFromPDU(req.GetTo())
	if to == nil {
		return errors.New("`To` must not be nil")
	} else if !to.IsSnapshot() {
		return errors.New("`To` must be a snapshot")
	}

	// create placeholder parent filesystems as appropriate
	//
	// Manipulating the ZFS dataset hierarchy must happen exclusively.
	// TODO: Use fine-grained locking to allow separate clients / requests to pass
	// 		 through the following section concurrently when operating on disjoint
	//       ZFS dataset hierarchy subtrees.
	var visitErr error
	func() {
		getLogger(ctx).Debug("begin acquire recvParentCreationMtx")
		defer s.recvParentCreationMtx.Lock().Unlock()
		getLogger(ctx).Debug("end acquire recvParentCreationMtx")
		defer getLogger(ctx).Debug("release recvParentCreationMtx")

		f := zfs.NewDatasetPathForest()
		f.Add(lp)
		getLogger(ctx).Debug("begin tree-walk")
		f.WalkTopDown(func(v *zfs.DatasetPathVisit) (visitChildTree bool) {
			if v.Path.Equal(lp) {
				return false
			}

			l := getLogger(ctx).
				WithField("placeholder_fs", v.Path.ToString()).
				WithField("receive_fs", lp.ToString())

			ph, err := zfs.ZFSGetFilesystemPlaceholderState(ctx, v.Path)
			l.WithField("placeholder_state", fmt.Sprintf("%#v", ph)).
				WithField("err", fmt.Sprintf("%s", err)).
				WithField("errType", fmt.Sprintf("%T", err)).
				Debug("get placeholder state for filesystem")
			if err != nil {
				visitErr = fmt.Errorf(
					"cannot get placeholder state of %s: %w",
					v.Path.ToString(), err)
				return false
			}

			if !ph.FSExists {
				if s.conf.RootWithoutClientComponent.HasPrefix(v.Path) {
					if v.Path.Length() == 1 {
						visitErr = fmt.Errorf("pool %q not imported",
							v.Path.ToString())
					} else {
						visitErr = fmt.Errorf("root_fs %q does not exist",
							s.conf.RootWithoutClientComponent.ToString())
					}
					l.WithError(visitErr).Error(
						"placeholders are only created automatically below root_fs")
					return false
				}

				// compute the value lazily so that users who don't rely on placeholders
				// can use the default value
				// PlaceholderCreationEncryptionPropertyUnspecified
				placeholderEncryption, err := s.receive_GetPlaceholderCreationEncryptionValue(root, v.Path)
				if err != nil {
					// logger already contains path
					l.WithError(err).Error("cannot create placeholder filesystem")
					visitErr = fmt.Errorf(
						"cannot create placeholder filesystem %s: %w",
						v.Path.ToString(), err)
					return false
				}

				l := l.WithField("encryption", placeholderEncryption)

				l.Debug("creating placeholder filesystem")
				err = zfs.ZFSCreatePlaceholderFilesystem(ctx,
					v.Path, v.Parent.Path, placeholderEncryption)
				if err != nil {
					// logger already contains path
					l.WithError(err).Error("cannot create placeholder filesystem")
					visitErr = fmt.Errorf(
						"cannot create placeholder filesystem %s: %w",
						v.Path.ToString(), err)
					return false
				}
				l.Info("created placeholder filesystem")
				return true
			} else {
				l.Debug("filesystem exists")
				return true // leave this fs as is
			}
		})
	}()

	getLogger(ctx).WithField("visitErr", visitErr).Debug("complete tree-walk")
	if visitErr != nil {
		return visitErr
	}

	log := getLogger(ctx).
		WithField("proto_fs", req.GetFilesystem()).
		WithField("local_fs", lp.ToString())

	// determine whether we need to rollback the filesystem / change its
	// placeholder state
	ph, err := zfs.ZFSGetFilesystemPlaceholderState(ctx, lp)
	if err != nil {
		return fmt.Errorf("cannot get placeholder state: %w", err)
	}
	log.WithField("placeholder_state", fmt.Sprintf("%#v", ph)).
		Debug("placeholder state")

	recvOpts := zfs.RecvOptions{
		SavePartialRecvState: true,
		InheritProperties:    s.conf.InheritProperties,
		OverrideProperties:   s.conf.OverrideProperties,
	}

	var clearPlaceholderProperty bool
	if ph.FSExists && ph.IsPlaceholder {
		recvOpts.RollbackAndForceRecv = true
		clearPlaceholderProperty = true
	}

	if clearPlaceholderProperty {
		log.Info("clearing placeholder property")
		if err := zfs.ZFSSetPlaceholder(ctx, lp, false); err != nil {
			return fmt.Errorf(
				"cannot clear placeholder property for forced receive: %w", err)
		}
	}

	if req.ClearResumeToken && ph.FSExists {
		log.Info("clearing resume token")
		if err := zfs.ZFSRecvClearResumeToken(ctx, lp.ToString()); err != nil {
			return fmt.Errorf("cannot clear resume token: %w", err)
		}
	}

	log.WithField("opts", fmt.Sprintf("%#v", recvOpts)).
		Debug("start receive command")

	snapFullPath := to.FullPath(lp.ToString())
	err = zfs.ZFSRecv(ctx, lp.ToString(), to, receive, recvOpts,
		s.conf.ExecPipe...)
	if err != nil {
		log.WithError(err).WithField("opts", fmt.Sprintf("%#v", recvOpts)).
			Error("zfs receive failed")
		return err
	}
	receive.Close()

	// validate that we actually received what the sender claimed
	toRecvd, err := to.ValidateExistsAndGetVersion(ctx, lp.ToString())
	if err != nil {
		msg := "receive request's `To` version does not match what we received in the stream"
		log.WithError(err).WithField("snap", snapFullPath).Error(msg)
		log.Error(
			"aborting recv request, but keeping received snapshot for inspection")
		return fmt.Errorf("%s: %w", msg, err)
	}

	replicationGuaranteeOptions, err := replicationGuaranteeOptionsFromPDU(
		req.GetReplicationConfig().Protection)
	if err != nil {
		return err
	}
	replicationGuaranteeStrategy := replicationGuaranteeOptions.
		Strategy(ph.FSExists)
	liveAbs, err := replicationGuaranteeStrategy.
		ReceiverPostRecv(ctx, s.conf.JobID, lp.ToString(), toRecvd)
	if err != nil {
		return err
	}
	for _, a := range liveAbs {
		if a != nil {
			abstractionsCacheSingleton.Put(a)
		}
	}
	keep := func(a Abstraction) (keep bool) {
		keep = false
		for _, k := range liveAbs {
			keep = keep || AbstractionEquals(a, k)
		}
		return keep
	}
	check := func(obsoleteAbs []Abstraction) {
		for _, abs := range obsoleteAbs {
			if zfs.FilesystemVersionEqualIdentity(abs.GetFilesystemVersion(), toRecvd) {
				panic(fmt.Sprintf(
					"would destroy endpoint abstraction around the filesystem version we just received %s",
					abs))
			}
		}
	}
	destroyTypes := AbstractionTypeSet{
		AbstractionLastReceivedHold: true,
	}
	abstractionsCacheSingleton.TryBatchDestroy(ctx, s.conf.JobID,
		lp.ToString(), destroyTypes, keep, check)
	return nil
}

func (s *Receiver) DestroySnapshots(ctx context.Context, req *pdu.DestroySnapshotsReq) (*pdu.DestroySnapshotsRes, error) {
	lp, err := mapToLocal(s.clientRootFromCtx(ctx), req.Filesystem)
	if err != nil {
		return nil, err
	}
	return doDestroySnapshots(ctx, lp, req.Snapshots)
}

func (p *Receiver) SendCompleted(ctx context.Context, _ *pdu.SendCompletedReq,
) error {
	return nil
}

func doDestroySnapshots(ctx context.Context, lp *zfs.DatasetPath, snaps []*pdu.FilesystemVersion) (*pdu.DestroySnapshotsRes, error) {
	reqs := make([]*zfs.DestroySnapOp, len(snaps))
	ress := make([]*pdu.DestroySnapshotRes, len(snaps))
	errs := make([]error, len(snaps))
	for i, fsv := range snaps {
		if fsv.Type != pdu.FilesystemVersion_Snapshot {
			return nil, fmt.Errorf("version %q is not a snapshot", fsv.Name)
		}
		ress[i] = &pdu.DestroySnapshotRes{
			Snapshot: fsv,
			// Error set after batch operation
		}
		reqs[i] = &zfs.DestroySnapOp{
			Filesystem: lp.ToString(),
			Name:       fsv.Name,
			ErrOut:     &errs[i],
		}
	}
	zfs.ZFSDestroyFilesystemVersions(ctx, reqs)
	for i := range reqs {
		if errs[i] != nil {
			var de *zfs.DestroySnapshotsError
			if errors.As(errs[i], &de) && len(de.Reason) == 1 {
				ress[i].Error = de.Reason[0]
			} else {
				ress[i].Error = errs[i].Error()
			}
		}
	}
	return &pdu.DestroySnapshotsRes{
		Results: ress,
	}, nil
}
