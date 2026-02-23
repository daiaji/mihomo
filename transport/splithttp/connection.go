package splithttp

import (
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/buf"
)

type splitConn struct {
	writer     io.WriteCloser
	reader     io.ReadCloser
	remoteAddr net.Addr
	localAddr  net.Addr
	onClose    func()

	waitHandshake func() error
	handshakeOnce sync.Once
	handshakeErr  error

	closeOnce  sync.Once
	readMu     sync.Mutex
	writeMu    sync.Mutex
	readTimer  *time.Timer
	writeTimer *time.Timer
}

func (c *splitConn) Write(b []byte) (int, error) { return c.writer.Write(b) }

func (c *splitConn) Read(b []byte) (int, error) {
	// 必须确保在 Read 之前握手已经完成
	c.handshakeOnce.Do(func() {
		if c.waitHandshake != nil {
			c.handshakeErr = c.waitHandshake()
		}
	})
	if c.handshakeErr != nil {
		return 0, c.handshakeErr
	}

	// 恢复标准的 io.Reader 调用
	return c.reader.Read(b)
}

// 实现 ReadBuffer 以减少内存拷贝
func (c *splitConn) ReadBuffer(buffer *buf.Buffer) error {
	c.handshakeOnce.Do(func() {
		if c.waitHandshake != nil {
			c.handshakeErr = c.waitHandshake()
		}
	})
	if c.handshakeErr != nil {
		return c.handshakeErr
	}

	// 核心修复：使用 Read 而不是 ReadFullFrom
	// ReadFullFrom 会一直读到 EOF 才返回，导致 TLS 握手挂死
	n, err := c.reader.Read(buffer.FreeBytes())
	if n > 0 {
		buffer.Advance(n)
	}
	return err
}

// // 告知框架此连接支持 Upstream 获取
// func (c *splitConn) Upstream() any {
// 	return c.reader
// }

// 告知框架此连接支持 Upstream 获取
func (c *splitConn) Upstream() any {
	return c.reader
}

func (c *splitConn) Close() error {
	var err1, err2 error
	c.closeOnce.Do(func() {
		if c.onClose != nil {
			c.onClose()
		}
		c.stopTimer(true)
		c.stopTimer(false)
		err1 = c.writer.Close()
		err2 = c.reader.Close()
	})
	if err1 != nil {
		return err1
	}
	return err2
}

func (c *splitConn) stopTimer(isRead bool) {
	if isRead {
		c.readMu.Lock()
		if c.readTimer != nil {
			c.readTimer.Stop()
			c.readTimer = nil
		}
		c.readMu.Unlock()
	} else {
		c.writeMu.Lock()
		if c.writeTimer != nil {
			c.writeTimer.Stop()
			c.writeTimer = nil
		}
		c.writeMu.Unlock()
	}
}

func (c *splitConn) LocalAddr() net.Addr  { return c.localAddr }
func (c *splitConn) RemoteAddr() net.Addr { return c.remoteAddr }

func (c *splitConn) SetDeadline(t time.Time) error {
	_ = c.SetReadDeadline(t)
	_ = c.SetWriteDeadline(t)
	return nil
}

func (c *splitConn) SetReadDeadline(t time.Time) error {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	if c.readTimer != nil {
		c.readTimer.Stop()
	}
	if t.IsZero() {
		c.readTimer = nil
		return nil
	}
	c.readTimer = time.AfterFunc(time.Until(t), func() {
		if pr, ok := c.reader.(*io.PipeReader); ok {
			_ = pr.CloseWithError(os.ErrDeadlineExceeded)
		}
	})
	return nil
}

func (c *splitConn) SetWriteDeadline(t time.Time) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.writeTimer != nil {
		c.writeTimer.Stop()
	}
	if t.IsZero() {
		c.writeTimer = nil
		return nil
	}
	c.writeTimer = time.AfterFunc(time.Until(t), func() {
		if pw, ok := c.writer.(*io.PipeWriter); ok {
			_ = pw.CloseWithError(os.ErrDeadlineExceeded)
		}
	})
	return nil
}
