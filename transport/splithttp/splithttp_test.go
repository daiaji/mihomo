package splithttp

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/http"
	"github.com/metacubex/mihomo/common/buf"
	"github.com/stretchr/testify/assert"
)

// echoHandler 模拟一个简单的回声服务器，将接收到的数据原样发回
func echoHandler(c net.Conn) {
	defer c.Close()
	_, _ = io.Copy(c, c)
}

// getTestConfig 生成测试专用的归一化配置
func getTestConfig(mode string) *Config {
	return &Config{
		Path:                 "/xhttp",
		Mode:                 mode,
		UplinkHTTPMethod:     "POST",
		ScMaxEachPostBytes:   &RangeConfig{From: 1024, To: 2048},
		ScMinPostsIntervalMs: &RangeConfig{From: 10, To: 10},
		ScMaxBufferedPosts:   30,
		XPaddingBytes:        &RangeConfig{From: 16, To: 32},
	}
}

// TestUploadQueue_Reassembly 测试上传队列的乱序重组功能
func TestUploadQueue_Reassembly(t *testing.T) {
	q := NewUploadQueue(10)
	b1 := buf.As([]byte("world"))
	b0 := buf.As([]byte("hello "))

	// 故意乱序推送：先推 Seq 1
	_ = q.Push(Packet{Buffer: b1, Seq: 1})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		res := make([]byte, 20)
		// 第一次 Read 应该阻塞，直到 Seq 0 到达
		n, _ := q.Read(res)
		assert.Equal(t, "hello ", string(res[:n]))
		// 第二次 Read 应该直接拿到已缓存的 Seq 1
		n, _ = q.Read(res)
		assert.Equal(t, "world", string(res[:n]))
	}()

	time.Sleep(50 * time.Millisecond)
	// 推送 Seq 0，触发重组
	_ = q.Push(Packet{Buffer: b0, Seq: 0})
	wg.Wait()
	_ = q.Close()
}

// TestSplitHTTP_EndToEnd 测试出站传输层与入站处理逻辑之间的端到端数据传输
func TestSplitHTTP_EndToEnd(t *testing.T) {
	modes := []string{"stream-one", "packet-up"}
	for _, mode := range modes {
		t.Run(mode, func(t *testing.T) {
			config := getTestConfig(mode)
			handler := NewServerHandler(config, echoHandler)

			mux := http.NewServeMux()
			mux.Handle(config.GetNormalizedPath(), handler)

			// 建立一个支持 H2C 的测试服务器
			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}

			protocols := new(http.Protocols)
			protocols.SetHTTP1(true)
			protocols.SetUnencryptedHTTP2(true) // 必须开启 H2C 以支持出站端的 HTTP/2 拨号

			server := &http.Server{
				Handler:   mux,
				Protocols: protocols,
			}
			go server.Serve(l)
			defer l.Close()

			// 配置出站传输层
			host := l.Addr().String()
			config.Host = host
			dialFn := func(ctx context.Context, network, addr string) (net.Conn, error) {
				return net.Dial("tcp", host)
			}

			// ✨ 修复点：NewTransport 现在需要 9 个参数。
			// 参数列表：dialFn, lpFn, tlsCfg, cfg, fp, authCert, authKey, echCfg, realityCfg
			tw := NewTransport(dialFn, nil, nil, config, "", "", "", nil, nil)
			defer tw.Close()

			// 执行拨号
			conn, err := tw.DialContext(context.Background())
			assert.NoError(t, err)

			// 写入测试数据
			msg := []byte("hello-xhttp-" + mode)
			_, err = conn.Write(msg)
			assert.NoError(t, err)

			// 读取回显数据并验证内容完整性
			recv := make([]byte, len(msg))
			_, err = io.ReadFull(conn, recv)
			assert.NoError(t, err)

			// 核心断言：验证传输后的数据是否与发送数据完全一致
			assert.Equal(t, msg, recv, "Data content mismatch in mode: "+mode)

			conn.Close()
		})
	}
}
