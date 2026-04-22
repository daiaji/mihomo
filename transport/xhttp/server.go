package xhttp

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/httputils"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/pool"

	"github.com/metacubex/http"
	"github.com/metacubex/http/h2c"
)

type ServerOption struct {
	Config
	ConnHandler func(net.Conn)
	HttpHandler http.Handler
}

type httpServerConn struct {
	mu      sync.Mutex
	w       http.ResponseWriter
	flusher http.Flusher
	reader  io.ReadCloser
	closed  bool
	done    chan struct{}
	once    sync.Once
}

func newHTTPServerConn(w http.ResponseWriter, r io.ReadCloser) *httpServerConn {
	flusher, _ := w.(http.Flusher)
	return &httpServerConn{
		w:       w,
		flusher: flusher,
		reader:  r,
		done:    make(chan struct{}),
	}
}

func (c *httpServerConn) Read(b []byte) (int, error) {
	return c.reader.Read(b)
}

func (c *httpServerConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return 0, io.ErrClosedPipe
	}

	n, err := c.w.Write(b)
	if err == nil && c.flusher != nil {
		c.flusher.Flush()
	}
	return n, err
}

func (c *httpServerConn) Close() error {
	c.once.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		close(c.done)
	})
	return c.reader.Close()
}

func (c *httpServerConn) Wait() <-chan struct{} {
	return c.done
}

type httpSession struct {
	uploadQueue *UploadQueue
	connected   chan struct{}
	once        sync.Once
}

func newHTTPSession(maxPackets int) *httpSession {
	return &httpSession{
		uploadQueue: NewUploadQueue(maxPackets),
		connected:   make(chan struct{}),
	}
}

func (s *httpSession) markConnected() {
	s.once.Do(func() {
		close(s.connected)
	})
}

type requestHandler struct {
	config      Config
	connHandler func(net.Conn)
	httpHandler http.Handler

	xPaddingBytes        Range
	scMaxEachPostBytes   Range
	scStreamUpServerSecs Range
	scMaxBufferedPosts   Range

	mu       sync.Mutex
	sessions map[string]*httpSession
}

func NewServerHandler(opt ServerOption) (http.Handler, error) {
	xPaddingBytes, err := opt.Config.GetNormalizedXPaddingBytes()
	if err != nil {
		return nil, err
	}
	scMaxEachPostBytes, err := opt.Config.GetNormalizedScMaxEachPostBytes()
	if err != nil {
		return nil, err
	}
	scStreamUpServerSecs, err := opt.Config.GetNormalizedScStreamUpServerSecs()
	if err != nil {
		return nil, err
	}
	scMaxBufferedPosts, err := opt.Config.GetNormalizedScMaxBufferedPosts()
	if err != nil {
		return nil, err
	}
	// using h2c.NewHandler to ensure we can work in plain http2
	// and some tls conn is not *tls.Conn (like *reality.Conn)
	return h2c.NewHandler(&requestHandler{
		config:               opt.Config,
		connHandler:          opt.ConnHandler,
		httpHandler:          opt.HttpHandler,
		xPaddingBytes:        xPaddingBytes,
		scMaxEachPostBytes:   scMaxEachPostBytes,
		scStreamUpServerSecs: scStreamUpServerSecs,
		scMaxBufferedPosts:   scMaxBufferedPosts,
		sessions:             map[string]*httpSession{},
	}, &http.Http2Server{
		IdleTimeout: 30 * time.Second,
	}), nil
}

func (h *requestHandler) upsertSession(sessionID string) *httpSession {
	h.mu.Lock()
	defer h.mu.Unlock()

	s, ok := h.sessions[sessionID]
	if ok {
		return s
	}

	s = newHTTPSession(h.scMaxBufferedPosts.Max)
	h.sessions[sessionID] = s

	// Reap orphan sessions that never become fully connected (e.g. from probing).
	// Matches Xray-core's 30-second reaper in upsertSession.
	go func() {
		timer := time.NewTimer(30 * time.Second)
		defer timer.Stop()
		select {
		case <-timer.C:
			h.deleteSession(sessionID)
		case <-s.connected:
		}
	}()

	return s
}

func (h *requestHandler) deleteSession(sessionID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if s, ok := h.sessions[sessionID]; ok {
		_ = s.uploadQueue.Close()
		delete(h.sessions, sessionID)
	}
}

func (h *requestHandler) normalizedMode() string {
	if h.config.Mode == "" {
		return "auto"
	}
	return h.config.Mode
}

func (h *requestHandler) allowStreamOne() bool {
	switch h.normalizedMode() {
	case "auto", "stream-one", "stream-up":
		return true
	default:
		return false
	}
}

func (h *requestHandler) allowSessionDownload() bool {
	switch h.normalizedMode() {
	case "auto", "stream-up", "packet-up":
		return true
	default:
		return false
	}
}

func (h *requestHandler) allowStreamUpUpload() bool {
	switch h.normalizedMode() {
	case "auto", "stream-up":
		return true
	default:
		return false
	}
}

func (h *requestHandler) allowPacketUpUpload() bool {
	switch h.normalizedMode() {
	case "auto", "packet-up":
		return true
	default:
		return false
	}
}

func (h *requestHandler) decodeMetadataToBuffer(r *http.Request, key string, isCookie bool) ([]byte, error) {
	totalEncodedLen := 0
	type chunk struct {
		val string
	}
	var chunks []chunk
	for i := 0; ; i++ {
		var encoded string
		if isCookie {
			cookieName := fmt.Sprintf("%s_%d", key, i)
			if c, _ := r.Cookie(cookieName); c != nil {
				encoded = c.Value
			}
		} else {
			encoded = r.Header.Get(fmt.Sprintf("%s-%d", key, i))
		}

		if encoded == "" {
			break
		}
		totalEncodedLen += len(encoded)
		chunks = append(chunks, chunk{val: encoded})
	}

	if totalEncodedLen == 0 {
		return nil, nil
	}

	decLen := base64.RawURLEncoding.DecodedLen(totalEncodedLen)
	target := pool.Get(decLen)
	offset := 0
	for _, c := range chunks {
		n, err := base64.RawURLEncoding.Decode(target[offset:], []byte(c.val))
		if err != nil {
			_ = pool.Put(target)
			return nil, err
		}
		offset += n
	}
	return target[:offset], nil
}

func (h *requestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := h.config.NormalizedPath()
	if h.httpHandler != nil && !strings.HasPrefix(r.URL.Path, path) {
		h.httpHandler.ServeHTTP(w, r)
		return
	}

	if h.config.Host != "" && !equalHost(r.Host, h.config.Host) {
		http.NotFound(w, r)
		return
	}

	if !strings.HasPrefix(r.URL.Path, path) {
		http.NotFound(w, r)
		return
	}

	h.config.WriteResponseHeader(w, r.Method, r.Header)
	length := h.xPaddingBytes.Rand()
	config := XPaddingConfig{Length: length}

	if h.config.XPaddingObfsMode {
		config.Placement = XPaddingPlacement{
			Placement: h.config.XPaddingPlacement,
			Key:       h.config.XPaddingKey,
			Header:    h.config.XPaddingHeader,
		}
		config.Method = PaddingMethod(h.config.XPaddingMethod)
	} else {
		config.Placement = XPaddingPlacement{
			Placement: PlacementHeader,
			Header:    "X-Padding",
		}
	}

	h.config.ApplyXPaddingToResponse(w, config)

	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	paddingValue, _ := h.config.ExtractXPaddingFromRequest(r, h.config.XPaddingObfsMode)
	if !h.config.IsPaddingValid(paddingValue, h.xPaddingBytes.Min, h.xPaddingBytes.Max, PaddingMethod(h.config.XPaddingMethod)) {
		http.Error(w, "invalid xpadding", http.StatusBadRequest)
		return
	}
	sessionId, seqStr := h.config.ExtractMetaFromRequest(r, path)

	var currentSession *httpSession
	if sessionId != "" {
		currentSession = h.upsertSession(sessionId)
	}

	// stream-up upload: POST /path/{session}
	if r.Method != http.MethodGet && sessionId != "" && seqStr == "" && h.allowStreamUpUpload() {
		httpSC := newHTTPServerConn(w, r.Body)
		err := currentSession.uploadQueue.Push(Packet{
			Reader: httpSC,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}

		// magic header instructs nginx + apache to not buffer response body
		w.Header().Set("X-Accel-Buffering", "no")
		// A web-compliant header telling all middleboxes to disable caching.
		// Should be able to prevent overloading the cache, or stop CDNs from
		// teeing the response stream into their cache, causing slowdowns.
		w.Header().Set("Cache-Control", "no-store")
		if !h.config.NoSSEHeader {
			// magic header to make the HTTP middle box consider this as SSE to disable buffer
			w.Header().Set("Content-Type", "text/event-stream")
		}
		w.WriteHeader(http.StatusOK)

		rc := http.NewResponseController(w)
		_ = rc.EnableFullDuplex() // http1 need to enable full duplex manually
		_ = rc.Flush()            // force flush the response header

		referrer := r.Header.Get("Referer")
		if referrer != "" && h.scStreamUpServerSecs.Max > 0 {
			go func() {
				for {
					_, err := httpSC.Write(bytes.Repeat([]byte{'X'}, int(h.xPaddingBytes.Rand())))
					if err != nil {
						break
					}
					time.Sleep(time.Duration(h.scStreamUpServerSecs.Rand()) * time.Second)
				}
			}()
		}

		select {
		case <-r.Context().Done():
		case <-httpSC.Wait():
		}

		_ = httpSC.Close()
		return
	}

	// packet-up upload: POST /path/{session}/{seq}
	if r.Method != http.MethodGet && sessionId != "" && seqStr != "" && h.allowPacketUpUpload() {
		h.handlePacketUp(w, r, currentSession, seqStr)
		return
	}

	// stream-up/packet-up download: GET /path/{session}
	if r.Method == http.MethodGet && sessionId != "" && seqStr == "" && h.allowSessionDownload() {
		currentSession.markConnected()

		// magic header instructs nginx + apache to not buffer response body
		w.Header().Set("X-Accel-Buffering", "no")
		// A web-compliant header telling all middleboxes to disable caching.
		// Should be able to prevent overloading the cache, or stop CDNs from
		// teeing the response stream into their cache, causing slowdowns.
		w.Header().Set("Cache-Control", "no-store")
		if !h.config.NoSSEHeader {
			// magic header to make the HTTP middle box consider this as SSE to disable buffer
			w.Header().Set("Content-Type", "text/event-stream")
		}
		w.WriteHeader(http.StatusOK)

		rc := http.NewResponseController(w)
		_ = rc.EnableFullDuplex() // http1 need to enable full duplex manually
		_ = rc.Flush()            // force flush the response header

		httpSC := newHTTPServerConn(w, r.Body)
		conn := &Conn{
			writer: httpSC,
			reader: currentSession.uploadQueue,
			onClose: func() {
				h.deleteSession(sessionId)
			},
		}
		httputils.SetAddrFromRequest(&conn.NetAddr, r)

		go h.connHandler(N.NewDeadlineConn(conn))

		select {
		case <-r.Context().Done():
		case <-httpSC.Wait():
		}

		_ = conn.Close()
		return
	}

	// stream-one: POST /path
	if r.Method != http.MethodGet && sessionId == "" && seqStr == "" && h.allowStreamOne() {
		w.Header().Set("X-Accel-Buffering", "no")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)

		rc := http.NewResponseController(w)
		_ = rc.EnableFullDuplex() // http1 need to enable full duplex manually
		_ = rc.Flush()            // force flush the response header

		httpSC := newHTTPServerConn(w, r.Body)
		conn := &Conn{
			writer: httpSC,
			reader: httpSC,
		}
		httputils.SetAddrFromRequest(&conn.NetAddr, r)

		go h.connHandler(N.NewDeadlineConn(conn))

		select {
		case <-r.Context().Done():
		case <-httpSC.Wait():
		}

		_ = conn.Close()
		return
	}

	http.NotFound(w, r)
}

func (h *requestHandler) handlePacketUp(w http.ResponseWriter, r *http.Request, currentSession *httpSession, seqStr string) {
	dataPlacement := h.config.GetNormalizedUplinkDataPlacement()
	uplinkDataKey := h.config.UplinkDataKey
	maxSize := h.scMaxEachPostBytes.Max

	var headerPayload, cookiePayload, bodyPayload []byte
	var err error

	// 1 & 2. Header/Cookie
	if dataPlacement == PlacementAuto || dataPlacement == PlacementHeader {
		headerPayload, _ = h.decodeMetadataToBuffer(r, uplinkDataKey, false)
	}
	if dataPlacement == PlacementAuto || dataPlacement == PlacementCookie {
		cookiePayload, _ = h.decodeMetadataToBuffer(r, uplinkDataKey, true)
	}

	// 3. Handle Body: completely abolish io.ReadAll
	if dataPlacement == PlacementAuto || dataPlacement == PlacementBody {
		cl := int(r.ContentLength)
		if cl > maxSize {
			h.releaseAll(headerPayload, cookiePayload, nil)
			http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
			return
		}

		if cl > 0 {
			bodyPayload = pool.Get(cl)
			_, err = io.ReadFull(r.Body, bodyPayload)
		} else if cl < 0 {
			// Chunked transfer: use LimitReader with pooled buffer
			tempBuf := pool.Get(maxSize)
			n, rerr := io.ReadFull(io.LimitReader(r.Body, int64(maxSize)), tempBuf)
			if rerr != nil && rerr != io.EOF && rerr != io.ErrUnexpectedEOF {
				_ = pool.Put(tempBuf)
				h.releaseAll(headerPayload, cookiePayload, nil)
				http.Error(w, "read chunked body failed", http.StatusBadRequest)
				return
			}
			bodyPayload = tempBuf[:n]
		}
		if err != nil {
			h.releaseAll(headerPayload, cookiePayload, bodyPayload)
			http.Error(w, "read body failed", http.StatusBadRequest)
			return
		}
	}

	// 4. Merge Payload: avoid redundant allocations
	var finalPayload []byte
	if dataPlacement == PlacementAuto {
		totalSize := len(headerPayload) + len(cookiePayload) + len(bodyPayload)
		if totalSize > maxSize {
			h.releaseAll(headerPayload, cookiePayload, bodyPayload)
			http.Error(w, "total payload too large", http.StatusRequestEntityTooLarge)
			return
		}
		if totalSize > 0 {
			finalPayload = pool.Get(totalSize)
			n := copy(finalPayload, headerPayload)
			n += copy(finalPayload[n:], cookiePayload)
			copy(finalPayload[n:], bodyPayload)
		}
		h.releaseAll(headerPayload, cookiePayload, bodyPayload) // Release fragments
	} else {
		// Assign directly
		switch dataPlacement {
		case PlacementHeader:
			finalPayload = headerPayload
		case PlacementCookie:
			finalPayload = cookiePayload
		case PlacementBody:
			finalPayload = bodyPayload
		}
	}

	// 5. Submit to queue
	seq, _ := strconv.ParseUint(seqStr, 10, 64)
	if err := currentSession.uploadQueue.Push(Packet{Seq: seq, Payload: finalPayload}); err != nil {
		if finalPayload != nil {
			_ = pool.Put(finalPayload)
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if len(finalPayload) == 0 {
		// Methods without a body are usually cached by default.
		w.Header().Set("Cache-Control", "no-store")
	}
	w.WriteHeader(http.StatusOK)
}

// Auxiliary release function
func (h *requestHandler) releaseAll(p1, p2, p3 []byte) {
	if p1 != nil {
		_ = pool.Put(p1)
	}
	if p2 != nil {
		_ = pool.Put(p2)
	}
	if p3 != nil {
		_ = pool.Put(p3)
	}
}

func equalHost(a, b string) bool {
	a = strings.ToLower(a)
	b = strings.ToLower(b)

	if ah, _, err := net.SplitHostPort(a); err == nil {
		a = ah
	}
	if bh, _, err := net.SplitHostPort(b); err == nil {
		b = bh
	}

	return a == b
}
