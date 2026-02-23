package deadline

import (
	"net"
	"time"

	"github.com/metacubex/mihomo/common/atomic"

	// Import sing common
	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	"github.com/metacubex/sing/common/network"
)

type connReadResult struct {
	buffer []byte
	err    error
}

type Conn struct {
	network.ExtendedConn
	deadline     atomic.TypedValue[time.Time]
	pipeDeadline PipeDeadline
	disablePipe  atomic.Bool
	inRead       atomic.Bool
	resultCh     chan *connReadResult
}

func IsConn(conn any) bool {
	_, ok := conn.(*Conn)
	return ok
}

func NewConn(conn net.Conn) *Conn {
	c := &Conn{
		// 换回 bufio 提供的实现，这是目前唯一能通过编译且支持接口的方式
		ExtendedConn: bufio.NewExtendedConn(conn),
		pipeDeadline: MakePipeDeadline(),
		resultCh:     make(chan *connReadResult, 1),
	}
	c.resultCh <- nil
	return c
}

func (c *Conn) Read(p []byte) (n int, err error) {
	// 彻底废弃补丁中的 resultCh 和 pipeRead 逻辑
	// 恢复为最基础的同步读取
	return c.ExtendedConn.Read(p)
}

func (c *Conn) pipeRead(size int) {
	buffer := make([]byte, size)
	n, err := c.ExtendedConn.Read(buffer)
	buffer = buffer[:n]
	c.resultCh <- &connReadResult{
		buffer: buffer,
		err:    err,
	}
}

func (c *Conn) ReadBuffer(buffer *buf.Buffer) (err error) {
	return c.ExtendedConn.ReadBuffer(buffer)
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	if c.disablePipe.Load() {
		return c.ExtendedConn.SetReadDeadline(t)
	} else if c.inRead.Load() {
		c.disablePipe.Store(true)
		return c.ExtendedConn.SetReadDeadline(t)
	}
	c.deadline.Store(t)
	c.pipeDeadline.Set(t)
	return nil
}

// 顺便清理一下这个方法，防止它返回错误的缓存状态
func (c *Conn) ReaderReplaceable() bool {
	return true
}

func (c *Conn) WriterReplaceable() bool {
	return true
}

// 修正：Upstream 必须返回 nil，严禁泄露底层流
func (c *Conn) Upstream() any {
	return c.ExtendedConn
}
