package job

import (
	"errors"
	"fmt"

	"github.com/dsh2dsh/zrepl/internal/config"
	"github.com/dsh2dsh/zrepl/internal/daemon/filters"
	"github.com/dsh2dsh/zrepl/internal/endpoint"
	"github.com/dsh2dsh/zrepl/internal/zfs"
)

type SendingJobConfig interface {
	GetFilesystems() (config.FilesystemsFilter, []config.DatasetFilter)
	GetSendOptions() *config.SendOptions // must not be nil
}

func buildSenderConfig(in SendingJobConfig, jobID endpoint.JobID) (*endpoint.SenderConfig, error) {
	fsf, err := filters.NewFromConfig(in.GetFilesystems())
	if err != nil {
		return nil, fmt.Errorf("cannot build filesystem filter: %w", err)
	}
	sendOpts := in.GetSendOptions()

	sc := &endpoint.SenderConfig{
		FSF:   fsf,
		JobID: jobID,

		ListPlaceholders: sendOpts.ListPlaceholders,

		Encrypt:              sendOpts.Encrypted,
		SendRaw:              sendOpts.Raw,
		SendProperties:       sendOpts.SendProperties,
		SendBackupProperties: sendOpts.BackupProperties,
		SendLargeBlocks:      sendOpts.LargeBlocks,
		SendCompressed:       sendOpts.Compressed,
		SendEmbeddedData:     sendOpts.EmbeddedData,
		SendSaved:            sendOpts.Saved,

		ExecPipe: sendOpts.ExecPipe,
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

	placeholderEncryption, err := endpoint.
		PlaceholderCreationEncryptionPropertyString(
			recvOpts.Placeholder.Encryption)
	if err != nil {
		propValues := endpoint.PlaceholderCreationEncryptionPropertyValues()
		options := make([]string, len(propValues))
		for i := range propValues {
			options[i] = propValues[i].String()
		}
		return rc, fmt.Errorf(
			"placeholder encryption value %q is invalid, must be one of %s",
			recvOpts.Placeholder.Encryption, options)
	}

	rc = endpoint.ReceiverConfig{
		JobID:                      jobID,
		RootWithoutClientComponent: rootFs,
		AppendClientIdentity:       in.GetAppendClientIdentity(),

		InheritProperties:     recvOpts.Properties.Inherit,
		OverrideProperties:    recvOpts.Properties.Override,
		PlaceholderEncryption: placeholderEncryption,

		ExecPipe: recvOpts.ExecPipe,
	}

	if err = rc.Validate(); err != nil {
		err = fmt.Errorf("cannot build receiver config: %w", err)
	}
	return rc, err
}
