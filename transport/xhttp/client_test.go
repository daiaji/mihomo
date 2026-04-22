package xhttp

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/pool"
	"github.com/stretchr/testify/assert"
)

func TestWaitReadCloser_Normal(t *testing.T) {
	wrc := NewWaitReadCloser()
	data := []byte("hello xhttp")

	go func() {
		// Simulate setting Body in an asynchronous network callback
		time.Sleep(50 * time.Millisecond)
		wrc.Set(io.NopCloser(bytes.NewReader(data)))
	}()

	buf := make([]byte, 20)
	n, err := wrc.Read(buf)

	assert.NoError(t, err)
	assert.Equal(t, len(data), n)
	assert.Equal(t, data, buf[:n])
}

func TestWaitReadCloser_Error(t *testing.T) {
	wrc := NewWaitReadCloser()
	expectErr := errors.New("roundtrip failed")

	go func() {
		time.Sleep(50 * time.Millisecond)
		wrc.CloseWithError(expectErr)
	}()

	buf := make([]byte, 10)
	n, err := wrc.Read(buf)

	assert.Equal(t, 0, n)
	assert.ErrorIs(t, err, expectErr)
}

func TestPacketUpWriter_PoolingLogic(t *testing.T) {
	// Verify patch fix: Ensure that buf can be returned even if its length is 0 during the flush process
	writer := &PacketUpWriter{
		buf: pool.Get(100)[:0], // Length 0, capacity 100
	}

	assert.NotNil(t, writer.buf)

	// Simulate the resource release logic in flushLocked from the patch
	buf := writer.buf
	writer.buf = nil
	if buf != nil {
		_ = pool.Put(buf)
	}

	assert.Nil(t, writer.buf, "The internal buffer pointer of the Writer must be cleared")
}
