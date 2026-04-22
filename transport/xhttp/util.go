package xhttp

import (
	"io"

	"github.com/metacubex/mihomo/common/pool"
)

// drainAndCloseBody drains and closes the body using a pooled buffer.
func drainAndCloseBody(body io.ReadCloser) {
	if body == nil {
		return
	}
	// Use mihomo predefined RelayBufferSize (32KB)
	buf := pool.Get(pool.RelayBufferSize)
	_, _ = io.CopyBuffer(io.Discard, body, buf)
	_ = pool.Put(buf)
	_ = body.Close()
}
