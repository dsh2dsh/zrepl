package job

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/dsh2dsh/zrepl/internal/daemon/logging"
)

type Connected interface {
	Name() string
	Endpoint() Endpoint

	PreHook(ctx context.Context) error
	PostHook(ctx context.Context) error
}

func newLocalConnected(listenerName, clientIdentity string,
	getter func(name string) *PassiveSide,
) *localConnected {
	return &localConnected{
		listenerName:   listenerName,
		clientIdentity: clientIdentity,
		getter:         getter,
	}
}

type localConnected struct {
	listenerName   string
	clientIdentity string

	getter func(name string) *PassiveSide
}

var _ Connected = (*localConnected)(nil)

func (self *localConnected) Name() string { return self.listenerName }

func (self *localConnected) Endpoint() Endpoint {
	return self.job().Endpoint(self.clientIdentity)
}

func (self *localConnected) job() *PassiveSide {
	j := self.getter(self.listenerName)
	if j == nil {
		panic(fmt.Sprintf(
			"local job with listener_name=%q not found", self.listenerName))
	}
	return j
}

func (self *localConnected) PreHook(ctx context.Context) error {
	return self.job().PreHook(self.hookContext(ctx), self.clientIdentity)
}

func (self *localConnected) hookContext(ctx context.Context) context.Context {
	log := GetLogger(ctx).With(
		slog.String("client_identity", self.clientIdentity))
	return logging.WithLogger(ctx, log)
}

func (self *localConnected) PostHook(ctx context.Context) error {
	return self.job().PostHook(self.hookContext(ctx), self.clientIdentity)
}

func newServerConnected(name string, client *Client) *serverConnected {
	return &serverConnected{name: name, client: client}
}

type serverConnected struct {
	name   string
	client *Client
}

var _ Connected = (*serverConnected)(nil)

func (self *serverConnected) Name() string { return self.name }

func (self *serverConnected) Endpoint() Endpoint { return self.client }

func (self *serverConnected) PreHook(ctx context.Context) error {
	return self.client.PreHook(ctx)
}

func (self *serverConnected) PostHook(ctx context.Context) error {
	return self.client.PostHook(ctx)
}
