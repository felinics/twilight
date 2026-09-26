package speech

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

const (
	headerTimestamp = "X-Timestamp"
)

// Edge TTS WebSocket client.
// Reference: https://github.com/readest/readest/blob/main/apps/readest-app/src/libs/edgeTTS.ts
//
// Protocol flow:
//
//	Client  ── Establish Connection ──────────>  Edge TTS Server
//	Client  ── speech.config (JSON) ─────────>  Edge TTS Server
//	Client  ── ssml (XML) ───────────────────>  Edge TTS Server
//	Client  <─ turn.start / response (Text) ──  Edge TTS Server
//	Client  <─ Audio binary frames ───────────  Edge TTS Server
//	Client  <─ turn.end (Text) ───────────────  Edge TTS Server
//
// Important implementation notes:
//
//   - TLS must negotiate HTTP/1.1 (NextProtos: ["http/1.1"]); the server returns 404 on HTTP/2.
//   - Do NOT set Sec-WebSocket-Version in headers; gorilla/websocket adds it automatically
//     and a duplicate causes "duplicate header not allowed" errors.
//   - ConnectionId (URL param) and X-RequestId (SSML header) MUST be the same 32-hex value.
//   - Each synthesis uses a one-shot connection: connect → config → SSML → audio → turn.end → close.
//     The server closes the WebSocket after turn.end, so connections cannot be reused.
//   - Binary audio frames use big-endian for the 2-byte header-length prefix.
//   - speech.config message must include an X-Timestamp header.
//   - SSML must declare xmlns:mstts and set xml:lang from the voice name.

// audioConfig holds the Edge-specific audio configuration extracted from SpeechParams.Config.
type audioConfig struct {
	Voice    string
	Language string
	Format   string
	Speed    float64
	Pitch    float64
}

type edgeWsClient struct {
	conn         *websocket.Conn
	connID       string
	mu           sync.Mutex
	outputFormat string
	BaseURL      string // for mock; empty uses the real Edge endpoint
}

func newEdgeWsClient() *edgeWsClient {
	return &edgeWsClient{
		outputFormat: "audio-24khz-48kbitrate-mono-mp3",
	}
}

func generateSecMSGec() string {
	ticks := time.Now().Unix() + winEpochOffset
	ticks -= ticks % 300
	ticks100ns := ticks * (sToNS / 100)
	strToHash := fmt.Sprintf("%d%s", ticks100ns, edgeAPIToken)
	sum := sha256.Sum256([]byte(strToHash))
	return strings.ToUpper(hex.EncodeToString(sum[:]))
}

func generateMuid() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return strings.ToUpper(hex.EncodeToString(b))
}

func (c *edgeWsClient) buildWsURL() string {
	base := edgeSpeechURL
	if c.BaseURL != "" {
		base = c.BaseURL
	}
	u, _ := url.Parse(base)
	q := u.Query()
	q.Set("TrustedClientToken", edgeAPIToken)
	q.Set("Sec-MS-GEC", generateSecMSGec())
	q.Set("Sec-MS-GEC-Version", "1-"+chromiumFullVersion)
	q.Set("ConnectionId", c.connID)
	u.RawQuery = q.Encode()
	return u.String()
}

func buildWSSHeaders() http.Header {
	h := http.Header{}
	h.Set("User-Agent",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"+
			" (KHTML, like Gecko) Chrome/"+chromiumMajorVersion+".0.0.0 Safari/537.36"+
			" Edg/"+chromiumMajorVersion+".0.0.0")
	h.Set("Accept-Encoding", "gzip, deflate, br, zstd")
	h.Set("Accept-Language", "en-US,en;q=0.9")
	h.Set("Pragma", "no-cache")
	h.Set("Cache-Control", "no-cache")
	h.Set("Origin", wssOrigin)
	h.Set("Cookie", "muid="+generateMuid()+";")
	return h
}

// connect establishes a new WebSocket connection to Edge TTS.
// Each call generates a fresh connID (one-connection-per-request model).
func (c *edgeWsClient) connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		return nil
	}
	c.connID = strings.ReplaceAll(uuid.New().String(), "-", "")

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		NextProtos: []string{"http/1.1"},
	}
	d := websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		TLSClientConfig:  tlsConfig,
		HandshakeTimeout: 15 * time.Second,
	}
	wsURL := c.buildWsURL()
	reqHeader := buildWSSHeaders()
	conn, resp, err := d.DialContext(ctx, wsURL, reqHeader)
	if err != nil {
		if resp != nil {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			return fmt.Errorf("edge tts ws dial: %w (status=%s body=%s)", err, resp.Status, string(bytes.TrimSpace(body)))
		}
		return fmt.Errorf("edge tts ws dial: %w", err)
	}
	c.conn = conn
	return nil
}

func (c *edgeWsClient) resetLocked() error {
	conn := c.conn
	c.conn = nil
	if conn == nil {
		return nil
	}
	return conn.Close()
}

func (c *edgeWsClient) sendFrame(path, contentType, body string, extraHeaders map[string]string) error {
	var b strings.Builder
	b.WriteString("Path: ")
	b.WriteString(path)
	b.WriteString("\r\n")
	b.WriteString("Content-Type: ")
	b.WriteString(contentType)
	b.WriteString("\r\n")
	for k, v := range extraHeaders {
		b.WriteString(k)
		b.WriteString(": ")
		b.WriteString(v)
		b.WriteString("\r\n")
	}
	b.WriteString("\r\n")
	b.WriteString(body)
	return c.conn.WriteMessage(websocket.TextMessage, []byte(b.String()))
}

func (c *edgeWsClient) configure(ctx context.Context, cfg audioConfig) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return errors.New("edge tts: not connected")
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = c.conn.SetWriteDeadline(deadline)
		defer func() { _ = c.conn.SetWriteDeadline(time.Time{}) }()
	}
	format := c.outputFormat
	if cfg.Format != "" {
		format = cfg.Format
	}
	body := fmt.Sprintf(`{"context":{"synthesis":{"audio":{"metadataoptions":{"sentenceBoundaryEnabled":false,"wordBoundaryEnabled":true},"outputFormat":%q}}}}`, format)
	extra := map[string]string{
		headerTimestamp: time.Now().String(),
	}
	return c.sendFrame("speech.config", "application/json; charset=utf-8", body, extra)
}

func buildSSML(text, voice, lang string, speed, pitch float64) string {
	if voice == "" {
		voice = DefaultVoice
	}
	if lang == "" {
		lang = "en-US"
	}

	rate := 0
	if speed > 0 {
		rate = int((speed - 1) * 100)
	}
	rateStr := fmt.Sprintf("%+d%%", rate)
	pitchStr := fmt.Sprintf("%+dHz", int(pitch))

	return fmt.Sprintf(
		`<speak version="1.0" xmlns="http://www.w3.org/2001/10/synthesis" xmlns:mstts="https://www.w3.org/2001/mstts" xml:lang="%s">`+
			`<voice name="%s"><prosody rate="%s" pitch="%s">%s</prosody></voice></speak>`,
		lang, voice, rateStr, pitchStr, escapeSSML(text))
}

func escapeSSML(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, "'", "&apos;")
	s = strings.ReplaceAll(s, "\"", "&quot;")
	return s
}

// synthesize sends SSML and synchronously collects all audio data.
func (c *edgeWsClient) synthesize(ctx context.Context, text string, cfg audioConfig) ([]byte, error) {
	if err := c.connect(ctx); err != nil {
		return nil, err
	}
	if err := c.configure(ctx, cfg); err != nil {
		return nil, err
	}

	c.mu.Lock()
	conn := c.conn
	connID := c.connID
	c.mu.Unlock()
	if conn == nil {
		return nil, errors.New("edge tts: not connected")
	}

	ssml := buildSSML(text, cfg.Voice, cfg.Language, cfg.Speed, cfg.Pitch)
	extra := map[string]string{
		"X-RequestId":   connID,
		headerTimestamp: time.Now().String(),
	}
	if err := c.sendFrame("ssml", "application/ssml+xml", ssml, extra); err != nil {
		return nil, err
	}

	var out []byte
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		mt, data, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				if len(out) > 0 {
					c.mu.Lock()
					_ = c.resetLocked()
					c.mu.Unlock()
					return out, nil
				}
			}
			c.mu.Lock()
			_ = c.resetLocked()
			c.mu.Unlock()
			return nil, fmt.Errorf("edge tts read (format=%q voice=%q): %w", cfg.Format, cfg.Voice, err)
		}
		switch mt {
		case websocket.TextMessage:
			if parsePath(data) == "turn.end" {
				c.mu.Lock()
				_ = c.resetLocked()
				c.mu.Unlock()
				return out, nil
			}
		case websocket.BinaryMessage:
			if audio := parseAudioChunk(data); len(audio) > 0 {
				out = append(out, audio...)
			}
		}
	}
}

func parsePath(data []byte) string {
	idx := bytes.Index(data, []byte("Path:"))
	if idx < 0 {
		return ""
	}
	lineEnd := bytes.Index(data[idx:], []byte("\r\n"))
	if lineEnd < 0 {
		lineEnd = len(data) - idx
	}
	pathLine := data[idx+5 : idx+lineEnd]
	return strings.TrimSpace(string(pathLine))
}

// parseAudioChunk parses an Edge binary audio frame: 2-byte big-endian header length + header + audio data.
func parseAudioChunk(data []byte) []byte {
	if len(data) < 2 {
		return nil
	}
	headerLen := binary.BigEndian.Uint16(data[:2])
	audioStart := 2 + int(headerLen)
	if audioStart > len(data) {
		return nil
	}
	return data[audioStart:]
}

// stream sends SSML and returns audio chunks via channel.
func (c *edgeWsClient) stream(ctx context.Context, text string, cfg audioConfig) (ch chan []byte, errCh chan error) {
	ch = make(chan []byte, 8)
	errCh = make(chan error, 1)
	go func() {
		defer close(ch)
		defer close(errCh)

		if err := c.connect(ctx); err != nil {
			errCh <- err
			return
		}
		if err := c.configure(ctx, cfg); err != nil {
			errCh <- err
			return
		}

		c.mu.Lock()
		conn := c.conn
		connID := c.connID
		c.mu.Unlock()
		if conn == nil {
			errCh <- errors.New("edge tts: not connected")
			return
		}

		ssml := buildSSML(text, cfg.Voice, cfg.Language, cfg.Speed, cfg.Pitch)
		extra := map[string]string{
			"X-RequestId":   connID,
			headerTimestamp: time.Now().String(),
		}
		if err := c.sendFrame("ssml", "application/ssml+xml", ssml, extra); err != nil {
			errCh <- err
			return
		}

		for {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			default:
			}
			mt, data, err := conn.ReadMessage()
			if err != nil {
				errCh <- fmt.Errorf("edge tts read: %w", err)
				return
			}
			switch mt {
			case websocket.TextMessage:
				if parsePath(data) == "turn.end" {
					c.mu.Lock()
					_ = c.resetLocked()
					c.mu.Unlock()
					return
				}
			case websocket.BinaryMessage:
				audio := parseAudioChunk(data)
				if len(audio) > 0 {
					select {
					case ch <- audio:
					case <-ctx.Done():
						errCh <- ctx.Err()
						return
					}
				}
			}
		}
	}()
	return ch, errCh
}
