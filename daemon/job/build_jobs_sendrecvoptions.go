package job

import (
	"errors"
	"fmt"

	"github.com/dsh2dsh/zrepl/config"
	"github.com/dsh2dsh/zrepl/daemon/filters"
	"github.com/dsh2dsh/zrepl/endpoint"
	"github.com/dsh2dsh/zrepl/util/nodefault"
	"github.com/dsh2dsh/zrepl/zfs"
)

type SendingJobConfig interface {
	GetFilesystems() config.FilesystemsFilter
	GetSendOptions() *config.SendOptions // must not be nil
}

func buildSenderConfig(in SendingJobConfig, jobID endpoint.JobID) (*endpoint.SenderConfig, error) {
	fsf, err := filters.DatasetMapFilterFromConfig(in.GetFilesystems())
	if err != nil {
		return nil, fmt.Errorf("cannot build filesystem filter: %w", err)
	}
	sendOpts := in.GetSendOptions()
	bwlim, err := buildBandwidthLimitConfig(&sendOpts.BandwidthLimit)
	if err != nil {
		return nil, fmt.Errorf("cannot build bandwidth limit config: %w", err)
	}

	sc := &endpoint.SenderConfig{
		FSF:   fsf,
		JobID: jobID,

		Encrypt:              &nodefault.Bool{B: sendOpts.Encrypted},
		SendRaw:              sendOpts.Raw,
		SendProperties:       sendOpts.SendProperties,
		SendBackupProperties: sendOpts.BackupProperties,
		SendLargeBlocks:      sendOpts.LargeBlocks,
		SendCompressed:       sendOpts.Compressed,
		SendEmbeddedData:     sendOpts.EmbeddedData,
		SendSaved:            sendOpts.Saved,

		BandwidthLimit: bwlim,
		ExecPipe:       sendOpts.ExecPipe,
	}

	if err := sc.Validate(); err != nil {
		return nil, fmt.Errorf("cannot build sender config: %w", err)
	}

	return sc, nil
}

type ReceivingJobConfig interface {
	GetRootFS() string
	GetAppendClientIdentity() bool
	GetRecvOptions() *config.RecvOptions
}

func buildReceiverConfig(in ReceivingJobConfig, jobID endpoint.JobID,
) (rc endpoint.ReceiverConfig, err error) {
	rootFs, err := zfs.NewDatasetPath(in.GetRootFS())
	if err != nil {
		return rc, errors.New("root_fs is not a valid zfs filesystem path")
	} else if rootFs.Length() <= 0 {
		// duplicates error check of receiver
		return rc, errors.New("root_fs must not be empty")
	}

	recvOpts := in.GetRecvOptions()

	bwlim, err := buildBandwidthLimitConfig(&recvOpts.BandwidthLimit)
	if err != nil {
		return rc, fmt.Errorf("cannot build bandwidth limit config: %w", err)
	}

	placeholderEncryption, err := endpoint.
		PlaceholderCreationEncryptionPropertyString(
			recvOpts.Placeholder.Encryption)
	if err != nil {
		options := []string{}
		for _, v := range endpoint.PlaceholderCreationEncryptionPropertyValues() {
			options = append(options,
				endpoint.PlaceholderCreationEncryptionProperty(v).String())
		}
		return rc, fmt.Errorf(
			"placeholder encryption value %q is invalid, must be one of %s",
			recvOpts.Placeholder.Encryption, options)
	}

	rc = endpoint.ReceiverConfig{
		JobID:                      jobID,
		RootWithoutClientComponent: rootFs,
		AppendClientIdentity:       in.GetAppendClientIdentity(),

		InheritProperties:  recvOpts.Properties.Inherit,
		OverrideProperties: recvOpts.Properties.Override,

		BandwidthLimit: bwlim,

		PlaceholderEncryption: placeholderEncryption,
		ExecPipe:              recvOpts.ExecPipe,
	}

	if err = rc.Validate(); err != nil {
		err = fmt.Errorf("cannot build receiver config: %w", err)
	}
	return
}
