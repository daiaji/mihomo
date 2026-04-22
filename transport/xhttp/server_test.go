package xhttp

import (
	"bytes"
	"encoding/base64"
	"io"
	"net"
	"net/url"
	"testing"

	"github.com/metacubex/http"
	"github.com/metacubex/http/httptest"
	"github.com/stretchr/testify/assert"
)

func TestServerHandlerModeRestrictions(t *testing.T) {
	testCases := []struct {
		name       string
		mode       string
		method     string
		target     string
		wantStatus int
	}{
		{
			name:       "StreamOneAcceptsStreamOne",
			mode:       "stream-one",
			method:     http.MethodPost,
			target:     "https://example.com/xhttp/",
			wantStatus: http.StatusOK,
		},
		{
			name:       "StreamOneRejectsSessionDownload",
			mode:       "stream-one",
			method:     http.MethodGet,
			target:     "https://example.com/xhttp/session",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "StreamUpAcceptsStreamOne",
			mode:       "stream-up",
			method:     http.MethodPost,
			target:     "https://example.com/xhttp/",
			wantStatus: http.StatusOK,
		},
		{
			name:       "StreamUpAllowsDownloadEndpoint",
			mode:       "stream-up",
			method:     http.MethodGet,
			target:     "https://example.com/xhttp/session",
			wantStatus: http.StatusOK,
		},
		{
			name:       "StreamUpRejectsPacketUpload",
			mode:       "stream-up",
			method:     http.MethodPost,
			target:     "https://example.com/xhttp/session/0",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "PacketUpAllowsDownloadEndpoint",
			mode:       "packet-up",
			method:     http.MethodGet,
			target:     "https://example.com/xhttp/session",
			wantStatus: http.StatusOK,
		},
		{
			name:       "PacketUpRejectsStreamOne",
			mode:       "packet-up",
			method:     http.MethodPost,
			target:     "https://example.com/xhttp/",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "PacketUpRejectsStreamUpUpload",
			mode:       "packet-up",
			method:     http.MethodPost,
			target:     "https://example.com/xhttp/session",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			config := Config{
				Path: "/xhttp",
				Mode: testCase.mode,
			}
			handler, err := NewServerHandler(ServerOption{
				Config: config,
				ConnHandler: func(conn net.Conn) {
					_ = conn.Close()
				},
			})
			assert.NoError(t, err)

			req := httptest.NewRequest(testCase.method, testCase.target, io.NopCloser(http.NoBody))
			recorder := httptest.NewRecorder()

			err = config.FillStreamRequest(req, "")
			assert.NoError(t, err)

			handler.ServeHTTP(recorder, req)

			assert.Equal(t, testCase.wantStatus, recorder.Result().StatusCode)
		})
	}
}

func TestHandlePacketUp_Detailed(t *testing.T) {
	config := Config{
		Path:                "/xhttp",
		UplinkDataPlacement: PlacementAuto,
		UplinkDataKey:       "x-data",
		XPaddingObfsMode:    true,
		XPaddingKey:         "padding",
		XPaddingMethod:      string(PaddingMethodRepeatX),
	}

	handler := &requestHandler{
		config:             config,
		scMaxEachPostBytes: Range{Min: 0, Max: 1024},
		xPaddingBytes:      Range{Min: 0, Max: 100},
	}

	t.Run("MergeHeaderAndBody", func(t *testing.T) {
		session := newHTTPSession(10)
		w := httptest.NewRecorder()

		// Construct: Header("ab") + Body("cd")
		// Note: xhttp internally uses base64.RawURLEncoding (no padding, URL-safe)
		headerData := base64.RawURLEncoding.EncodeToString([]byte("ab")) // Result is "YWI"
		body := []byte("cd")

		req := httptest.NewRequest("POST", "/xhttp/sess/0", bytes.NewReader(body))
		req.Header.Set("x-data-0", headerData)
		req.ContentLength = int64(len(body))

		// Inject valid padding to pass server check
		q := url.Values{}
		q.Set("padding", "XXXX")
		req.URL.RawQuery = q.Encode()

		handler.handlePacketUp(w, req, session, "0")

		assert.Equal(t, http.StatusOK, w.Code)

		// Verify the final payload in the queue
		readBuf := make([]byte, 100)
		n, err := session.uploadQueue.Read(readBuf)
		assert.NoError(t, err)
		assert.Equal(t, "abcd", string(readBuf[:n]))
	})

	t.Run("RejectTooLargeBody", func(t *testing.T) {
		session := newHTTPSession(10)
		w := httptest.NewRecorder()

		// Simulate a packet exceeding the maximum limit (1024)
		largeBody := bytes.Repeat([]byte{'A'}, 2000)
		req := httptest.NewRequest("POST", "/xhttp/sess/0", bytes.NewReader(largeBody))
		req.ContentLength = 2000

		q := url.Values{}
		q.Set("padding", "XXXX")
		req.URL.RawQuery = q.Encode()

		handler.handlePacketUp(w, req, session, "0")

		assert.Equal(t, http.StatusRequestEntityTooLarge, w.Code)
	})
}
