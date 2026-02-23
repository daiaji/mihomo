package splithttp

import (
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/buf"
	"github.com/metacubex/sing/common/bufio"
	N "github.com/metacubex/sing/common/network"
)

type splitConn struct {
	writer     io.WriteCloser
	reader     io.ReadCloser // 修正：恢复为 io.ReadCloser 以支持 Close() 并适配 transport.go 的赋值
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

func (c *splitConn) Write(b []byte) (int, error) {
	return c.writer.Write(b)
}

// WriteBuffer 实现 N.ExtendedConn 接口
func (c *splitConn) WriteBuffer(buffer *buf.Buffer) error {
	// 包装 writer 为 ExtendedWriter 并执行写入
	return bufio.NewExtendedWriter(c.writer).WriteBuffer(buffer)
}

func (c *splitConn) Read(b []byte) (int, error) {
	c.handshakeOnce.Do(func() {
		if c.waitHandshake != nil {
			c.handshakeErr = c.waitHandshake()
		}
	})
	if c.handshakeErr != nil {
		return 0, c.handshakeErr
	}

	return c.reader.Read(b)
}

// ReadBuffer 实现 N.ExtendedConn 接口，解决主线 deadline.go 的读取优化
func (c *splitConn) ReadBuffer(buffer *buf.Buffer) error {
	c.handshakeOnce.Do(func() {
		if c.waitHandshake != nil {
			c.handshakeErr = c.waitHandshake()
		}
	})
	if c.handshakeErr != nil {
		return c.handshakeErr
	}
	if c.reader == nil {
		return io.EOF
	}

	// ✨ 核心适配：动态包装并调用，避免主线异步抢占导致的数据截断
	return bufio.NewExtendedReader(c.reader).ReadBuffer(buffer)
}

// Upstream 适配
func (c *splitConn) Upstream() any {
	// ✨ 保持返回 nil。
	// 这会阻止主线 deadline.go 的 pipeRead 协程直接抢夺底层 HTTP 流的数据，
	// 从而修复 curl 出现的 unexpected eof 错误。
	return nil
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
		if c.reader != nil {
			err2 = c.reader.Close() // 现在正常了，因为类型是 io.ReadCloser
		}
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
		// 尝试使用接口关闭管道
		if pr, ok := c.reader.(interface{ CloseWithError(error) error }); ok {
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
		if pw, ok := c.writer.(interface{ CloseWithError(error) error }); ok {
			_ = pw.CloseWithError(os.ErrDeadlineExceeded)
		}
	})
	return nil
}

// 确保 splitConn 完全实现了 N.ExtendedConn 接口
var _ N.ExtendedConn = (*splitConn)(nil)
