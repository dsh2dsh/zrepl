package signal

import (
	"context"
	"errors"
)

var ErrGracefulStop = errors.New("graceful stop")

type ctxKeyGraceful struct{}

var gracefulCtxKey ctxKeyGraceful = struct{}{}

func WithGraceful(ctx context.Context, graceful context.Context,
) context.Context {
	return context.WithValue(ctx, gracefulCtxKey, graceful)
}

func GracefulFrom(ctx context.Context) context.Context {
	if graceful, ok := ctx.Value(gracefulCtxKey).(context.Context); ok {
		return graceful
	}
	return context.Background()
}
