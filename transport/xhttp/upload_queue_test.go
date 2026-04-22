package xhttp

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestUploadQueueMaxPackets verifies the core logic of the original queue: packet count limit, out-of-order handling, and read order
func TestUploadQueueMaxPackets(t *testing.T) {
	q := NewUploadQueue(2)
	ch := make(chan struct{})
	go func() {
		// Push data. After refactoring, the lifecycle of Payload is managed by q
		err := q.Push(Packet{Seq: 0, Payload: []byte{'0'}})
		assert.NoError(t, err)
		err = q.Push(Packet{Seq: 1, Payload: []byte{'1'}})
		assert.NoError(t, err)
		err = q.Push(Packet{Seq: 2, Payload: []byte{'2'}})
		assert.NoError(t, err)
		err = q.Push(Packet{Seq: 4, Payload: []byte{'4'}})
		assert.NoError(t, err)
		err = q.Push(Packet{Seq: 5, Payload: []byte{'5'}})
		assert.NoError(t, err)
		err = q.Push(Packet{Seq: 6, Payload: []byte{'6'}})
		assert.NoError(t, err)
		err = q.Push(Packet{Seq: 7, Payload: []byte{'7'}})
		assert.ErrorIs(t, err, io.ErrClosedPipe)
		close(ch)
	}()

	buf := make([]byte, 20)
	n, err := q.Read(buf)
	assert.Equal(t, 3, n)
	assert.Equal(t, []byte{'0', '1', '2'}, buf[:n])
	assert.NoError(t, err)

	// Seq 3 is missing, and because the number of cached packets exceeds maxPackets (2), ErrQueueTooLarge is triggered
	n, err = q.Read(buf)
	assert.Equal(t, 0, n)
	assert.ErrorIs(t, err, ErrQueueTooLarge)

	err = q.Close()
	assert.NoError(t, err)

	<-ch
}

// TestUploadQueue_ZeroLengthPayload verifies the handling of zero-length packets
// This is one of the most important parts of the audit: preventing the Read method from falling into an infinite loop due to handling len=0 q.buf
// At the same time, ensure that even if the length is 0 but the capacity is not 0, the packet is correctly recycled by checking the internal state
func TestUploadQueue_ZeroLengthPayload(t *testing.T) {

	q := NewUploadQueue(5)

	// 1. Push a Seq 0: simulate a Payload taken from the pool with its length reset to 0 (large capacity)
	emptyPayload := make([]byte, 0, 1024)
	_ = q.Push(Packet{Seq: 0, Payload: emptyPayload})

	// 2. Push a Seq 1: normal data packet
	_ = q.Push(Packet{Seq: 1, Payload: []byte("payload")})

	buf := make([]byte, 20)
	// The correct logic should automatically skip the empty packet (Seq 0) and directly return the content of Seq 1
	// If the logic is wrong, Read will get stuck at Seq 0 and keep returning (0, nil), making the test unable to continue
	n, err := q.Read(buf)

	assert.NoError(t, err)
	assert.Equal(t, 7, n)
	assert.Equal(t, []byte("payload"), buf[:n])

	// Verify state: After the empty packet is consumed, q.buf must be set to nil (triggering pool.Put)
	q.mu.Lock()
	assert.Nil(t, q.buf, "zero-length buffer should be cleared and returned to pool")
	q.mu.Unlock()
}

// TestUploadQueue_OffsetLogic specifically tests the bufOffset cursor logic introduced by the refactoring
// Ensure that under multiple non-aligned reads, the data stream can still be correctly restored without destroying the capacity information of the underlying slice
func TestUploadQueue_OffsetLogic(t *testing.T) {
	q := NewUploadQueue(5)

	// Construct a 100-byte Payload
	size := 100
	p := make([]byte, size)
	for i := 0; i < size; i++ {
		p[i] = byte(i)
	}

	_ = q.Push(Packet{Seq: 0, Payload: p})

	// Simulate segmented non-aligned reading: 30 bytes, 30 bytes, 40 bytes
	totalRead := 0
	steps := []int{30, 30, 40}
	for _, step := range steps {
		readBuf := make([]byte, step)
		n, err := q.Read(readBuf)
		assert.NoError(t, err)
		assert.Equal(t, step, n)

		// Verify if the read data fragments are accurate
		for i := 0; i < n; i++ {
			assert.Equal(t, byte(totalRead+i), readBuf[i])
		}
		totalRead += n
	}

	// Verify state machine: After all reading is finished, the internal cache pointer must be cleared
	q.mu.Lock()
	assert.Nil(t, q.buf, "internal buffer should be nil after fully read")
	assert.Equal(t, 0, q.bufOffset, "offset should be reset to 0")
	q.mu.Unlock()
}

// TestUploadQueue_CloseState verifies that UploadQueue correctly cleans up all internal resource references when closed
func TestUploadQueue_CloseState(t *testing.T) {
	q := NewUploadQueue(10)

	// 1. Push data to be processed into the packets map (Seq 0)
	_ = q.Push(Packet{Seq: 0, Payload: []byte("p0")})
	assert.Equal(t, 1, len(q.packets))

	// 2. Push Seq 1 and read the complete content of Seq 0, making Seq 1 the currently active q.buf
	_ = q.Push(Packet{Seq: 1, Payload: []byte("active-buffer")})

	tmp := make([]byte, 2)
	n, _ := q.Read(tmp) // Read Seq 0 ("p0")
	assert.Equal(t, 2, n)

	// Read 1 more byte to make q.bufOffset > 0, ensuring q.buf is being occupied
	tmp1 := make([]byte, 1)
	_, _ = q.Read(tmp1)

	q.mu.Lock()
	assert.NotNil(t, q.buf)
	assert.True(t, q.bufOffset > 0)
	assert.Equal(t, 0, len(q.packets), "Seq 1 should be moved from map to q.buf")
	q.mu.Unlock()

	// 3. Execute close
	_ = q.Close()

	// 4. Verify resource cleanup state: UploadQueue should no longer hold any data references (preventing memory leaks)
	q.mu.Lock()
	assert.Nil(t, q.buf, "active buffer should be cleared on close")
	assert.Equal(t, 0, len(q.packets), "pending packets map should be cleared on close")
	assert.True(t, q.closed)
	q.mu.Unlock()

	// 5. Verify behavior after close: subsequent Push should fail and not leak the newly pushed Payload
	pLate := []byte("too-late")
	errPush := q.Push(Packet{Seq: 2, Payload: pLate})
	assert.ErrorIs(t, errPush, io.ErrClosedPipe)
}

// TestUploadQueue_ReaderBehavior verifies Push behavior in stream read (Reader) mode
func TestUploadQueue_ReaderBehavior(t *testing.T) {
	q := NewUploadQueue(5)
	pr, pw := io.Pipe()

	// 1. Push a Reader (Stream-up mode)
	err := q.Push(Packet{Reader: pr})
	assert.NoError(t, err)

	// 2. Error should be reported when pushing Reader or Packet again while a Reader already exists (mutually exclusive modes)
	err2 := q.Push(Packet{Reader: pr})
	assert.Error(t, err2)
	assert.Contains(t, err2.Error(), "reader already exists")

	err3 := q.Push(Packet{Seq: 0, Payload: []byte("data")})
	assert.Error(t, err3)
	assert.Contains(t, err3.Error(), "reader already exists")

	_ = pw.Close()
	_ = q.Close()
}
