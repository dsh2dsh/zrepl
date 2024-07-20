package jsonclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
)

// RequestEditorFn is the function signature for the RequestEditor callback
// function.
type RequestEditorFn func(ctx context.Context, req *http.Request) error

// Doer performs HTTP requests.
//
// The standard http.Client implements this interface.
type HttpRequestDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Client which conforms to the OpenAPI3 specification for this service.
type Client struct {
	// The endpoint of the server conforming to this interface, with scheme,
	// https://api.deepmap.com for example. This can contain a path relative to
	// the server, such as https://api.deepmap.com/dev-test, and all the paths in
	// the swagger spec will be appended to the server.
	Server string

	// Doer for performing requests, typically a *http.Client with any customized
	// settings, such as certificate chains.
	Client HttpRequestDoer

	// A list of callbacks for modifying requests which are generated before
	// sending over the network.
	RequestEditors []RequestEditorFn
}

// ClientOption allows setting custom parameters during construction
type ClientOption func(*Client) error

// Creates a new Client, with reasonable defaults
func New(server string, opts ...ClientOption) (*Client, error) {
	// create a client with sane default values
	client := &Client{
		Server: server,
	}
	// mutate client and add all optional params
	for _, o := range opts {
		if err := o(client); err != nil {
			return nil, err
		}
	}
	// ensure the server URL always has a trailing slash
	if !strings.HasSuffix(client.Server, "/") {
		client.Server += "/"
	}
	// create httpClient, if not already present
	if client.Client == nil {
		client.Client = &http.Client{}
	}
	return client, nil
}

func NewUnix(sockPath string) (*Client, error) {
	return New("http://unix/", WithControlPath(sockPath))
}

func WithControlPath(sockPath string) ClientOption {
	return func(c *Client) error {
		absPath, err := filepath.Abs(sockPath)
		if err != nil {
			return fmt.Errorf("abs path: %w", err)
		}

		c.Client = &http.Client{
			Transport: &http.Transport{
				DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
					return net.Dial("unix", absPath)
				},
			},
		}
		return nil
	}
}

// WithHTTPClient allows overriding the default Doer, which is
// automatically created using http.Client. This is useful for tests.
func WithHTTPClient(doer HttpRequestDoer) ClientOption {
	return func(c *Client) error {
		c.Client = doer
		return nil
	}
}

// WithRequestEditorFn allows setting up a callback function, which will be
// called right before sending the request. This can be used to mutate the
// request.
func (self *Client) WithRequestEditorFn(fn RequestEditorFn) *Client {
	self.RequestEditors = append(self.RequestEditors, fn)
	return self
}

func (self *Client) Get(ctx context.Context, endpoint string, out any,
	reqEditors ...RequestEditorFn,
) error {
	req, err := self.NewRequest(ctx, http.MethodGet, endpoint, nil, reqEditors...)
	if err != nil {
		return err
	}
	return self.Do(req, out)
}

func (self *Client) NewRequest(ctx context.Context, method, endpoint string,
	body io.Reader, reqEditors ...RequestEditorFn,
) (*http.Request, error) {
	serverURL, err := url.Parse(self.Server)
	if err != nil {
		return nil, fmt.Errorf("parse url: %w", err)
	}

	if endpoint[0] == '/' {
		endpoint = "." + endpoint
	}

	queryURL, err := serverURL.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse url: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, method, queryURL.String(), body)
	if err != nil {
		return nil, fmt.Errorf("http req: %w", err)
	} else if err := self.applyEditors(ctx, req, reqEditors); err != nil {
		return nil, err
	}
	return req, nil
}

func (self *Client) applyEditors(ctx context.Context, req *http.Request,
	additionalEditors []RequestEditorFn,
) error {
	for _, r := range self.RequestEditors {
		if err := r(ctx, req); err != nil {
			return err
		}
	}
	for _, r := range additionalEditors {
		if err := r(ctx, req); err != nil {
			return err
		}
	}
	return nil
}

func (self *Client) Do(req *http.Request, out any) error {
	resp, err := self.Client.Do(req)
	if err != nil {
		return fmt.Errorf("http do: %w", err)
	}
	defer resp.Body.Close()

	var b bytes.Buffer
	if resp.StatusCode != http.StatusOK {
		// ignore error, just display what we got
		_, _ = io.CopyN(&b, resp.Body, 1024)
		return fmt.Errorf("unexpected response from %q: %v (%v)",
			req.URL, resp.Status, strings.TrimSpace(b.String()))
	}

	if out != nil && out != struct{}{} {
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return fmt.Errorf("marshal response: %w", err)
		}
	}
	return nil
}

func (self *Client) Post(ctx context.Context, endpoint string, in, out any,
	reqEditors ...RequestEditorFn,
) error {
	var body io.Reader
	if in != nil && in != struct{}{} {
		var b bytes.Buffer
		if err := json.NewEncoder(&b).Encode(in); err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		body = &b
	}

	req, err := self.NewRequest(ctx, http.MethodPost, endpoint, body,
		reqEditors...)
	if err != nil {
		return err
	}
	return self.Do(req, out)
}
