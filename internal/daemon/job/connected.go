package job

import "fmt"

type Connected interface {
	Name() string
	Endpoint() Endpoint
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
