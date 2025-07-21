package job

import (
	"context"
	"sync"

	"github.com/dsh2dsh/zrepl/internal/replication/logic"
	"github.com/dsh2dsh/zrepl/internal/replication/logic/pdu"
)

type Endpoint interface {
	logic.Endpoint
	logic.Receiver
	logic.Sender
}

func NewSenderOnce(ctx context.Context, endpoint logic.Sender,
) *SenderOnce {
	listFn := func() (*pdu.ListFilesystemRes, error) {
		return endpoint.ListFilesystems(ctx)
	}
	return &SenderOnce{
		Sender:              endpoint,
		listFilesystemsOnce: sync.OnceValues(listFn),
	}
}

type SenderOnce struct {
	logic.Sender

	listFilesystemsOnce func() (*pdu.ListFilesystemRes, error)
}

var _ logic.Sender = (*SenderOnce)(nil)

func (self *SenderOnce) ListFilesystems(context.Context,
) (*pdu.ListFilesystemRes, error) {
	return self.listFilesystemsOnce()
}
