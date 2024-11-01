package jsonclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
)

const jsonLenHeader = "X-Zrepl-Json-Length"

func (self *Client) PostStream(ctx context.Context, endpoint string,
	in, out any, r io.Reader, reqEditors ...RequestEditorFn,
) error {
	var b bytes.Buffer
	if err := json.NewEncoder(&b).Encode(in); err != nil {
		return fmt.Errorf("jsonclient: marshaling json payload: %w", err)
	}
	b.WriteString("\n")

	body := io.MultiReader(&b, r)
	editors := slices.Concat([]RequestEditorFn{
		func(ctx context.Context, req *http.Request) error {
			req.Header.Set("Content-Type", "application/octet-stream")
			req.Header.Set(jsonLenHeader, strconv.Itoa(b.Len()))
			return nil
		},
	}, reqEditors)

	req, err := self.NewRequest(ctx, http.MethodPost, endpoint, body, editors...)
	if err != nil {
		return err
	}
	return self.Do(req, out)
}

func (self *Client) PostResponseStream(ctx context.Context, endpoint string,
	in, out any, reqEditors ...RequestEditorFn,
) (io.ReadCloser, error) {
	req, err := self.postRequest(ctx, endpoint, in, reqEditors...)
	if err != nil {
		return nil, err
	}

	resp, err := self.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http do: %w", err)
	}

	if err := parseJsonPayload(resp, out); err != nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("unexpected response from %q: %w", req.URL, err)
	}
	return resp.Body, nil
}

func parseJsonPayload(resp *http.Response, out any) error {
	if err := checkStatusCode(resp); err != nil {
		return err
	} else if !canUnmarshal(out) {
		return nil
	}
	return ReadJsonPayload(resp.Header, resp.Body, out)
}

func ReadJsonPayload(h http.Header, r io.Reader, out any) error {
	lenStr := h.Get(jsonLenHeader)
	if lenStr == "" {
		return nil
	}

	jsonLen, err := strconv.Atoi(lenStr)
	if err != nil {
		return fmt.Errorf(
			"jsonclient: parsing json payload length %q: %w", lenStr, err)
	} else if jsonLen == 0 {
		return nil
	}

	lr := io.LimitReader(r, int64(jsonLen))
	if err = json.NewDecoder(lr).Decode(out); err != nil {
		return fmt.Errorf("jsonclient: decoding json payload %w", err)
	}
	return nil
}

func WriteJsonPayload(h http.Header, w io.Writer, in any) error {
	var b bytes.Buffer
	if err := json.NewEncoder(&b).Encode(in); err != nil {
		return fmt.Errorf("jsonclient: marshaling json payload: %w", err)
	}
	b.WriteString("\n")

	h.Set(jsonLenHeader, strconv.Itoa(b.Len()))
	if _, err := io.Copy(w, &b); err != nil {
		return fmt.Errorf("jsonclient: writing json payload: %w", err)
	}
	return nil
}
