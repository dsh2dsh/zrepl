package local

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dsh2dsh/zrepl/config"
	"github.com/dsh2dsh/zrepl/transport"
)

type LocalConnecter struct {
	listenerName   string
	clientIdentity string
	dialTimeout    time.Duration
}

func LocalConnecterFromConfig(in *config.LocalConnect) (*LocalConnecter, error) {
	if in.ClientIdentity == "" {
		return nil, errors.New("ClientIdentity must not be empty")
	}
	if in.ListenerName == "" {
		return nil, errors.New("ListenerName must not be empty")
	}
	if in.DialTimeout < 0 {
		return nil, errors.New("DialTimeout must be zero or positive")
	}
	cn := &LocalConnecter{
		listenerName:   in.ListenerName,
		clientIdentity: in.ClientIdentity,
		dialTimeout:    in.DialTimeout,
	}
	return cn, nil
}

func (c *LocalConnecter) Connect(dialCtx context.Context) (transport.Wire, error) {
	l := GetLocalListener(c.listenerName)
	if c.dialTimeout > 0 {
		ctx, cancel := context.WithTimeout(dialCtx, c.dialTimeout)
		defer cancel()
		dialCtx = ctx // shadow
	}
	w, err := l.Connect(dialCtx, c.clientIdentity)
	if err == context.DeadlineExceeded {
		return nil, fmt.Errorf("local listener %q not reachable", c.listenerName)
	}
	return w, err
}
