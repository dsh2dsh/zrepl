package client

import (
	"context"
	"fmt"

	"github.com/dsh2dsh/zrepl/client/jsonclient"
)

func jsonRequestResponse(sockPath, endpoint string, in, out any) error {
	jc, err := jsonclient.NewUnix(sockPath)
	if err != nil {
		return fmt.Errorf("new jsonclient: %w", err)
	}

	if in == nil || in == struct{}{} {
		if err := jc.Get(context.Background(), endpoint, out); err != nil {
			return fmt.Errorf("jsonclient get: %w", err)
		}
		return nil
	}

	if err := jc.Post(context.Background(), endpoint, in, out); err != nil {
		return fmt.Errorf("jsonclient post: %w", err)
	}
	return nil
}
