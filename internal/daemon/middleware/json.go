package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/dsh2dsh/zrepl/internal/client/jsonclient"
	"github.com/dsh2dsh/zrepl/internal/logger"
)

func JsonResponder[T any](h func(context.Context) (*T, error)) Middleware {
	fn := func(w http.ResponseWriter, r *http.Request) {
		resp, err := h(r.Context())
		if err != nil {
			writeError(w, r, err, "handler error")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			writeError(w, r, err, "json marshal error")
		}
	}
	return func(next http.Handler) http.Handler { return http.HandlerFunc(fn) }
}

func writeError(w http.ResponseWriter, r *http.Request, err error, msg string,
) {
	statusCode := http.StatusInternalServerError
	var httpErr *HttpError
	if errors.As(err, &httpErr) {
		statusCode = httpErr.StatusCode()
	}
	writeErrorCode(w, r, statusCode, err, msg)
}

func writeErrorCode(w http.ResponseWriter, r *http.Request, statusCode int,
	err error, msg string,
) {
	logger.WithError(getLogger(r), err, msg)
	http.Error(w, err.Error(), statusCode)
}

func JsonRequestResponder[T1, T2 any](h func(ctx context.Context, req *T1,
) (*T2, error),
) Middleware {
	fn := func(w http.ResponseWriter, r *http.Request) {
		var req T1
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErrorCode(w, r, http.StatusBadRequest, err,
				"json unmarshal error",
			)
			return
		}

		resp, err := h(r.Context(), &req)
		if err != nil {
			writeError(w, r, err, "handler error")
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			writeError(w, r, err, "json marshal error")
		}
	}
	return func(next http.Handler) http.Handler { return http.HandlerFunc(fn) }
}

// --------------------------------------------------

func JsonRequestStream[T any](h func(context.Context, *T, io.ReadCloser) error,
) Middleware {
	fn := func(w http.ResponseWriter, r *http.Request) {
		var req T
		err := jsonclient.ReadJsonPayload(r.Header, r.Body, &req)
		if err != nil {
			writeError(w, r, err, "error reading json payload")
			return
		}

		if err := h(r.Context(), &req, r.Body); err != nil {
			writeError(w, r, err, "error calling handler")
			return
		}
		w.WriteHeader(http.StatusOK)
	}
	return func(next http.Handler) http.Handler { return http.HandlerFunc(fn) }
}

func JsonRequestResponseStream[T1, T2 any](
	h func(context.Context, *T1) (*T2, io.ReadCloser, error),
) Middleware {
	fn := func(w http.ResponseWriter, r *http.Request) {
		var req T1
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErrorCode(w, r, http.StatusBadRequest, err,
				"json unmarshal error",
			)
			return
		}

		resp, stream, err := h(r.Context(), &req)
		if err != nil {
			writeError(w, r, err, "handler error")
			return
		}
		defer stream.Close()

		w.Header().Set("Content-Type", "application/octet-stream")
		if err := jsonclient.WriteJsonPayload(w.Header(), w, resp); err != nil {
			writeError(w, r, err, "error calling handler")
		} else if _, err = io.Copy(w, stream); err != nil {
			writeError(w, r, err, "error copying stream to response")
		}
	}
	return func(next http.Handler) http.Handler { return http.HandlerFunc(fn) }
}
