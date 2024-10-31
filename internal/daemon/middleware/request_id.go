package middleware

import (
	"context"
	"net/http"
	"strconv"
	"sync/atomic"

	"github.com/dsh2dsh/zrepl/internal/daemon/logging"
)

type ctxKeyRequestId struct{}

var RequestIdKey ctxKeyRequestId = struct{}{}

func genRequestId() uint64 {
	return atomic.AddUint64(&nextRequestId, 1)
}

var nextRequestId uint64

func RequestId(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		requestId := genRequestId()
		ctx := context.WithValue(r.Context(),
			RequestIdKey, strconv.FormatUint(requestId, 10))
		ctx = logging.WithLogger(ctx, getLogger(r).WithField("rid", requestId))
		next.ServeHTTP(w, r.WithContext(ctx))
	}
	return http.HandlerFunc(fn)
}

func RequestIdFrom(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if id, ok := ctx.Value(RequestIdKey).(string); ok {
		return id
	}
	return ""
}
