package middleware

import (
	"context"
	"encoding/json"
	"net/http"
)

func JsonResponder[T any](h func(context.Context) (T, error)) Middleware {
	fn := func(w http.ResponseWriter, r *http.Request) {
		res, err := h(r.Context())
		if err != nil {
			writeError(w, r, err, "control handler error")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(&res); err != nil {
			writeError(w, r, err, "control handler json marshal error")
		}
	}
	return func(next http.Handler) http.Handler { return http.HandlerFunc(fn) }
}

func writeError(w http.ResponseWriter, r *http.Request, err error, msg string,
) {
	writeErrorCode(w, r, http.StatusInternalServerError, err, msg)
}

func writeErrorCode(w http.ResponseWriter, r *http.Request, statusCode int,
	err error, msg string,
) {
	getLogger(r).WithError(err).Error(msg)
	http.Error(w, err.Error(), statusCode)
}

// --------------------------------------------------

func JsonRequestResponder[T1 any, T2 any](h func(ctx context.Context, req *T1,
) (T2, error),
) Middleware {
	fn := func(w http.ResponseWriter, r *http.Request) {
		var req T1
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErrorCode(w, r, http.StatusBadRequest, err,
				"control handler json unmarshal error",
			)
			return
		}

		resp, err := h(r.Context(), &req)
		if err != nil {
			writeError(w, r, err, "control handler error")
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(&resp); err != nil {
			writeError(w, r, err, "control handler json marshal error")
		}
	}
	return func(next http.Handler) http.Handler { return http.HandlerFunc(fn) }
}
