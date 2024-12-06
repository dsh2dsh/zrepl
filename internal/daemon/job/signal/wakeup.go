package signal

import "context"

type ctxKeyWakeup struct{}

var wakeupCtxKey ctxKeyWakeup = struct{}{}

func WithWakeup(ctx context.Context, wakeup context.Context) context.Context {
	return context.WithValue(ctx, wakeupCtxKey, wakeup)
}

func WakeupFrom(ctx context.Context) context.Context {
	if wakeup, ok := ctx.Value(wakeupCtxKey).(context.Context); ok {
		return wakeup
	}
	return context.Background()
}
