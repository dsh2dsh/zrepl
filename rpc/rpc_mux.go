package rpc

import (
	"context"
	"time"

	"github.com/dsh2dsh/zrepl/rpc/transportmux"
	"github.com/dsh2dsh/zrepl/transport"
	"github.com/dsh2dsh/zrepl/util/envconst"
)

type demuxedListener struct {
	control, data transport.AuthenticatedListener
}

const (
	transportmuxLabelControl string = "zrepl_control"
	transportmuxLabelData    string = "zrepl_data"
)

func demux(serveCtx context.Context, listener transport.AuthenticatedListener) demuxedListener {
	listeners, err := transportmux.Demux(
		serveCtx, listener,
		[]string{transportmuxLabelControl, transportmuxLabelData},
		envconst.Duration("ZREPL_TRANSPORT_DEMUX_TIMEOUT", 10*time.Second),
	)
	if err != nil {
		// transportmux API guarantees that the returned error can only be due
		// to invalid API usage (i.e. labels too long)
		panic(err)
	}
	return demuxedListener{
		control: listeners[transportmuxLabelControl],
		data:    listeners[transportmuxLabelData],
	}
}

type muxedConnecter struct {
	control, data transport.Connecter
}

func mux(rawConnecter transport.Connecter) muxedConnecter {
	muxedConnecters, err := transportmux.MuxConnecter(
		rawConnecter,
		[]string{transportmuxLabelControl, transportmuxLabelData},
		envconst.Duration("ZREPL_TRANSPORT_MUX_TIMEOUT", 10*time.Second),
	)
	if err != nil {
		// transportmux API guarantees that the returned error can only be due
		// to invalid API usage (i.e. labels too long)
		panic(err)
	}
	return muxedConnecter{
		control: muxedConnecters[transportmuxLabelControl],
		data:    muxedConnecters[transportmuxLabelData],
	}
}
