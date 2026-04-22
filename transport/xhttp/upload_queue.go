package xhttp

import (
	"errors"
	"io"
	"sync"

	"github.com/metacubex/mihomo/common/pool"
)

var ErrQueueTooLarge = errors.New("packet queue is too large")

type Packet struct {
	Seq     uint64
	Payload []byte // UploadQueue will hold Payload, so never reuse it after UploadQueue.Push
	Reader  io.ReadCloser
}

type UploadQueue struct {
	mu         sync.Mutex
	condPushed sync.Cond
	condPopped sync.Cond
	packets    map[uint64][]byte
	nextSeq    uint64
	buf        []byte
	bufOffset  int // Added: cursor, preventing reslice from destroying cap, ensuring it can be returned to the pool
	closed     bool
	maxPackets int
	reader     io.ReadCloser
}

func NewUploadQueue(maxPackets int) *UploadQueue {
	q := &UploadQueue{
		packets:    make(map[uint64][]byte, maxPackets),
		maxPackets: maxPackets,
	}
	q.condPushed = sync.Cond{L: &q.mu}
	q.condPopped = sync.Cond{L: &q.mu}
	return q
}

func (q *UploadQueue) Push(p Packet) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		// Determine p.Payload != nil instead of len > 0 to ensure that empty packets with capacity are also recycled
		if p.Payload != nil {
			_ = pool.Put(p.Payload)
		}
		return io.ErrClosedPipe
	}

	if q.reader != nil {
		if p.Payload != nil {
			_ = pool.Put(p.Payload)
		}
		return errors.New("uploadQueue.reader already exists")
	}

	if p.Reader != nil {
		q.reader = p.Reader
		q.condPushed.Broadcast()
		return nil
	}

	for len(q.packets) > q.maxPackets {
		q.condPopped.Wait() // wait for the reader to read the packets
		if q.closed {
			if p.Payload != nil {
				_ = pool.Put(p.Payload)
			}
			return io.ErrClosedPipe
		}
	}

	q.packets[p.Seq] = p.Payload
	q.condPushed.Broadcast()
	return nil
}

func (q *UploadQueue) Read(b []byte) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	totalRead := 0
	for {
		// 1. If there is a buffer currently being processed
		if q.buf != nil {
			n := copy(b[totalRead:], q.buf[q.bufOffset:])
			q.bufOffset += n
			totalRead += n
			if q.bufOffset >= len(q.buf) {
				// Read finished, return the entire memory (keep cap unchanged)
				_ = pool.Put(q.buf)
				q.buf = nil
				q.bufOffset = 0
			}
			if totalRead >= len(b) {
				return totalRead, nil
			}
		}

		// 2. Try to take the next packet from the packets map
		if payload, ok := q.packets[q.nextSeq]; ok {
			delete(q.packets, q.nextSeq)
			q.nextSeq++

			// Audit fix: if the packet length is 0 (possibly Keep-Alive), return it directly and skip
			// Otherwise, Read will keep returning (0, nil) because q.buf != nil, leading to an infinite loop
			if len(payload) == 0 {
				if payload != nil {
					_ = pool.Put(payload)
				}
				q.condPopped.Broadcast()
				continue // Look for the next Seq
			}

			q.buf = payload
			q.bufOffset = 0
			q.condPopped.Broadcast()
			continue // Enter the loop header to start copy
		}

		// 3. If data has already been read, return directly
		if totalRead > 0 {
			return totalRead, nil
		}

		// 4. Handle stream mode Reader
		if reader := q.reader; reader != nil {
			q.mu.Unlock() // unlock before calling q.reader.Read
			n, err := reader.Read(b)
			q.mu.Lock()
			return n, err
		}

		// 5. State determination
		if q.closed {
			return 0, io.EOF
		}

		if len(q.packets) > q.maxPackets {
			// "reassembly buffer" exceeds the limit
			return 0, ErrQueueTooLarge
		}

		q.condPushed.Wait()
	}
}

func (q *UploadQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	var err error
	if q.reader != nil {
		err = q.reader.Close()
	}

	// Audit fix: clean up all packets, even if len is 0, as long as the underlying layer has capacity, it must be recycled
	for seq, p := range q.packets {
		if p != nil {
			_ = pool.Put(p)
		}
		delete(q.packets, seq)
	}

	// Clean up currently held active cache
	if q.buf != nil {
		_ = pool.Put(q.buf)
		q.buf = nil
	}

	q.closed = true
	q.condPushed.Broadcast()
	q.condPopped.Broadcast()
	return err
}
