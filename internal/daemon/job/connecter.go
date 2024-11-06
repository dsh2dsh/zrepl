package job

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/dsh2dsh/zrepl/internal/client/jsonclient"
	"github.com/dsh2dsh/zrepl/internal/config"
)

func NewConnecter(keys []config.AuthKey) *Connecter {
	cn := &Connecter{
		jobs: make(map[string]*PassiveSide, 1),
		keys: make(map[string]config.AuthKey, len(keys)),

		httpClient: &http.Client{
			Transport: &http.Transport{IdleConnTimeout: 30 * time.Second},
		},
		timeout: time.Minute,

		requiredJobs: make([]string, 0, 1),
	}
	for i := range keys {
		key := &keys[i]
		cn.keys[key.Name] = keys[i]
	}
	return cn
}

type Connecter struct {
	jobs map[string]*PassiveSide
	keys map[string]config.AuthKey

	httpClient *http.Client
	timeout    time.Duration

	requiredJobs []string
}

func (self *Connecter) WithTimeout(d time.Duration) *Connecter {
	if d > 0 {
		self.timeout = d
	}
	return self
}

func (self *Connecter) AddJob(listnerName string, j *PassiveSide) {
	self.jobs[listnerName] = j
}

func (self *Connecter) Job(name string) *PassiveSide { return self.jobs[name] }

func (self *Connecter) FromConfig(in *config.LocalConnect) (Connected, error) {
	switch {
	case in.Type == "local":
		return self.newLocal(in.ListenerName, in.ClientIdentity), nil
	case in.Server != "":
		return self.newServer(in.Server, in.ListenerName, in.ClientIdentity)
	}
	return nil, fmt.Errorf("unknown type %q", in.Type)
}

func (self *Connecter) newLocal(listenerName, clientIdentity string,
) *localConnected {
	self.requiredJobs = append(self.requiredJobs, listenerName)
	return newLocalConnected(listenerName, clientIdentity,
		func(name string) *PassiveSide { return self.jobs[name] })
}

func (self *Connecter) newServer(server, listenerName, clientIdentity string,
) (*serverConnected, error) {
	authKey, ok := self.keys[clientIdentity]
	if !ok {
		return nil, fmt.Errorf("client_identity not found in keys: %q",
			clientIdentity)
	}

	authValue := "Bearer " + authKey.Key
	name := listenerName + "@" + server

	jsonClient, err := jsonclient.New(server,
		jsonclient.WithHTTPClient(self.httpClient),
		jsonclient.WithRequestEditorFn(
			func(_ context.Context, req *http.Request) error {
				req.Header.Set("Authorization", authValue)
				return nil
			}))
	if err != nil {
		return nil, fmt.Errorf("build jsonclient for %q: %w", name, err)
	}

	client := NewClient(listenerName, jsonClient).WithTimeout(self.timeout)
	cn := newServerConnected(name, client)
	return cn, nil
}

func (self *Connecter) Validate() error {
	for _, name := range self.requiredJobs {
		if j := self.Job(name); j == nil {
			return fmt.Errorf("Sink job %q required, but not registered.", name)
		}
	}
	return nil
}
