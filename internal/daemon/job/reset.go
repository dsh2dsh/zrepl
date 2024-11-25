package job

import (
	"context"
	"errors"
)

type resetCtxKey struct{}

var resetKey resetCtxKey = struct{}{}

func WaitReset(ctx context.Context) <-chan struct{} {
	wc, ok := ctx.Value(resetKey).(chan struct{})
	if !ok {
		wc = make(chan struct{})
	}
	return wc
}

var ErrAlreadyReset = errors.New("already reset")

func ContextWithReset(ctx context.Context) (context.Context, func() error) {
	wc := make(chan struct{})
	wuf := func() error {
		select {
		case wc <- struct{}{}:
			return nil
		default:
			return ErrAlreadyReset
		}
	}
	return context.WithValue(ctx, resetKey, wc), wuf
}
