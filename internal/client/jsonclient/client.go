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

func WithRequestEditorFn(fn RequestEditorFn) ClientOption {
	return func(c *Client) error {
		c.WithRequestEditorFn(fn)
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
	defer BodyClose(resp.Body)

	if err := checkStatusCode(resp); err != nil {
		return fmt.Errorf("unexpected response from %q: %w", req.URL, err)
	}
	return unmarshalBody(resp.Body, out)
}

func checkStatusCode(resp *http.Response) error {
	if resp.StatusCode == http.StatusOK {
		return nil
	}

	var b bytes.Buffer
	// ignore error, just display what we got
	_, _ = io.CopyN(&b, resp.Body, 1024)
	return fmt.Errorf("%v: %v", resp.Status, strings.TrimSpace(b.String()))
}

func unmarshalBody(r io.Reader, v any) error {
	if !canUnmarshal(v) {
		return nil
	} else if err := json.NewDecoder(r).Decode(&v); err != nil {
		return fmt.Errorf("unmarshal response: %w", err)
	}
	return nil
}

func canUnmarshal(v any) bool { return v != nil && v != struct{}{} }

func (self *Client) Post(ctx context.Context, endpoint string, in, out any,
	reqEditors ...RequestEditorFn,
) error {
	req, err := self.postRequest(ctx, endpoint, in, reqEditors...)
	if err != nil {
		return err
	}
	return self.Do(req, out)
}

func (self *Client) postRequest(ctx context.Context, endpoint string, in any,
	reqEditors ...RequestEditorFn,
) (*http.Request, error) {
	var body bytes.Buffer
	if canUnmarshal(in) {
		if err := json.NewEncoder(&body).Encode(in); err != nil {
			return nil, fmt.Errorf("marshal request: %w", err)
		}
	}
	return self.NewRequest(ctx, http.MethodPost, endpoint, &body, reqEditors...)
}

// maxPostHandlerReadBytes is the max number of Request.Body bytes not
// consumed by a handler that the server will read from the client
// in order to keep a connection alive. If there are more bytes
// than this, the server, to be paranoid, instead sends a
// "Connection close" response.
//
// This number is approximately what a typical machine's TCP buffer
// size is anyway.  (if we have the bytes on the machine, we might as
// well read them)
//
// See: net/http/server.go
const maxPostHandlerReadBytes = 256 << 10

// https://github.com/golang/go/issues/60240
func BodyClose(r io.ReadCloser) {
	_, _ = io.CopyN(io.Discard, r, maxPostHandlerReadBytes+1)
	r.Close()
}
