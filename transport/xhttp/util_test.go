package xhttp

import (
	"bytes"
	"io"
	"testing"

	"github.com/metacubex/mihomo/common/pool"
	"github.com/stretchr/testify/assert"
)

type mockReadCloser struct {
	io.Reader
	closed bool
}

func (m *mockReadCloser) Close() error {
	m.closed = true
	return nil
}

func TestDrainAndCloseBody(t *testing.T) {
	// Construct data exceeding the default size of the pooled buffer
	dataSize := pool.RelayBufferSize * 2
	data := bytes.Repeat([]byte{'A'}, dataSize)
	mc := &mockReadCloser{
		Reader: bytes.NewReader(data),
		closed: false,
	}

	drainAndCloseBody(mc)

	assert.True(t, mc.closed, "Body should be closed normally")

	// Verify that the data has been completely drained
	rem, _ := io.ReadAll(mc)
	assert.Equal(t, 0, len(rem), "Reader should not have any remaining data")
}

func TestDrainAndCloseBody_Nil(t *testing.T) {
	// Verify stability when handling nil pointers
	assert.NotPanics(t, func() {
		drainAndCloseBody(nil)
	})
}
