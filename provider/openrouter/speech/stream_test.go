package speech

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/felinics/twilight/provider/providertest"
	sdk "github.com/felinics/twilight/sdk"
)

// streamTimeout bounds every wait in these tests, so a regression to a
// buffering DoStream fails instead of hanging.
const streamTimeout = 5 * time.Second

// pcmBytes returns n bytes of distinct, non-silent sample data, so that a
// dropped, duplicated or reordered byte changes the result.
func pcmBytes(n, seed int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(seed*31 + i*7 + 1)
	}
	return b
}

// audioEvent returns one SSE event carrying b64 as delta.audio.data.
func audioEvent(b64 string) string {
	return fmt.Sprintf("data: {\"choices\":[{\"index\":0,\"delta\":{\"audio\":{\"data\":%q,\"transcript\":\"x\"}}}]}\n\n", b64)
}

// writeEvent writes one SSE event and flushes it to the client.
func writeEvent(t *testing.T, w http.ResponseWriter, event string) {
	t.Helper()
	_, _ = fmt.Fprint(w, event)
	w.(http.Flusher).Flush()
}

// startSSE sends the 200 status line of an SSE response.
func startSSE(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprint(w, ": OPENROUTER PROCESSING\n\n")
	w.(http.Flusher).Flush()
}

// receive reads one chunk from the stream, failing the test on timeout.
func receive(t *testing.T, stream <-chan []byte) ([]byte, bool) {
	t.Helper()
	select {
	case chunk, ok := <-stream:
		return chunk, ok
	case <-time.After(streamTimeout):
		t.Fatal("timed out waiting for a stream chunk")
		return nil, false
	}
}

// drain reads the stream until it closes, failing the test on timeout.
func drain(t *testing.T, stream <-chan []byte) [][]byte {
	t.Helper()
	var chunks [][]byte
	for {
		chunk, ok := receive(t, stream)
		if !ok {
			return chunks
		}
		chunks = append(chunks, chunk)
	}
}

// synthesizePCM runs DoSynthesize against a server that sends deltas at once
// and returns the PCM data of the WAV it produced.
func synthesizePCM(t *testing.T, deltas []string) []byte {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		startSSE(w)
		for _, d := range deltas {
			writeEvent(t, w, audioEvent(d))
		}
		writeEvent(t, w, "data: [DONE]\n\n")
	}))
	defer srv.Close()

	res, err := New(WithAPIKey("k"), WithBaseURL(srv.URL)).DoSynthesize(context.Background(), sdk.SpeechParams{Text: "hi"})
	if err != nil {
		t.Fatalf("DoSynthesize: %v", err)
	}
	if got := binary.LittleEndian.Uint32(res.Audio[40:44]); int(got) != len(res.Audio)-44 {
		t.Fatalf("DoSynthesize data size = %d, want %d", got, len(res.Audio)-44)
	}
	return res.Audio[44:]
}

// checkStreamedWAV checks that chunks form a streaming WAV: a header with
// unknown sizes on the first chunk, then PCM in whole samples except possibly
// at the very end. It returns the PCM data.
func checkStreamedWAV(t *testing.T, chunks [][]byte) []byte {
	t.Helper()
	if len(chunks) == 0 {
		t.Fatal("stream yielded no chunks")
	}
	joined := bytes.Join(chunks, nil)
	wantHeader := wavHeader(math.MaxUint32, pcmSampleRate)
	if len(chunks[0]) <= len(wantHeader) || !bytes.Equal(joined[:len(wantHeader)], wantHeader) {
		t.Fatalf("first chunk does not start with the streaming WAV header plus audio: % x", chunks[0][:min(len(chunks[0]), 48)])
	}
	if binary.LittleEndian.Uint32(joined[4:8]) != math.MaxUint32 || binary.LittleEndian.Uint32(joined[40:44]) != math.MaxUint32 {
		t.Fatal("streaming WAV header sizes are not 0xFFFFFFFF")
	}
	for i, c := range chunks[:len(chunks)-1] {
		n := len(c)
		if i == 0 {
			n -= len(wantHeader)
		}
		if n%2 != 0 {
			t.Errorf("chunk %d holds %d PCM bytes, not whole samples", i, n)
		}
	}
	return joined[len(wantHeader):]
}

func TestDoStreamForwardsAudioBeforeUpstreamFinishes(t *testing.T) {
	t.Parallel()
	pcm := [][]byte{pcmBytes(480, 1), pcmBytes(960, 2), pcmBytes(480, 3)}
	deltas := make([]string, len(pcm))
	for i, b := range pcm {
		deltas[i] = base64.StdEncoding.EncodeToString(b)
	}

	release := make(chan struct{})
	var gaveUp atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startSSE(w)
		writeEvent(t, w, audioEvent(deltas[0]))
		// Hold the rest of the response until the client has seen audio. A
		// DoStream that waits for the whole response only gets its first
		// chunk after this gives up.
		select {
		case <-release:
		case <-r.Context().Done():
			return
		case <-time.After(streamTimeout):
			gaveUp.Store(true)
			return
		}
		for _, d := range deltas[1:] {
			time.Sleep(10 * time.Millisecond)
			writeEvent(t, w, audioEvent(d))
		}
		writeEvent(t, w, "data: [DONE]\n\n")
	}))
	defer srv.Close()

	p := New(WithAPIKey("k"), WithBaseURL(srv.URL))
	sr, err := p.DoStream(context.Background(), sdk.SpeechParams{Text: "hi"})
	if err != nil {
		t.Fatalf("DoStream: %v", err)
	}
	if sr.ContentType != contentTypeAudio {
		t.Errorf("ContentType = %q, want %q", sr.ContentType, contentTypeAudio)
	}

	first, ok := receive(t, sr.Stream)
	if !ok {
		t.Fatal("stream closed before the first chunk")
	}
	if gaveUp.Load() {
		t.Fatal("first chunk arrived only after the upstream response ended")
	}
	close(release)
	chunks := append([][]byte{first}, drain(t, sr.Stream)...)
	if err := sr.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	if len(chunks) != len(deltas) {
		t.Errorf("got %d chunks, want one per delta (%d)", len(chunks), len(deltas))
	}

	got := checkStreamedWAV(t, chunks)
	if want := bytes.Join(pcm, nil); !bytes.Equal(got, want) {
		t.Fatal("streamed PCM differs from the PCM sent")
	}
	if want := synthesizePCM(t, deltas); !bytes.Equal(got, want) {
		t.Fatal("streamed PCM differs from the PCM DoSynthesize returns")
	}
}

func TestDoStreamOddLengthDeltas(t *testing.T) {
	t.Parallel()
	cases := map[string][]int{
		"even total": {3, 5, 1, 7},
		"odd total":  {3, 2, 1, 1, 4},
		"one byte":   {1},
	}
	for name, sizes := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var want []byte
			deltas := make([]string, len(sizes))
			for i, n := range sizes {
				b := pcmBytes(n, i)
				want = append(want, b...)
				deltas[i] = base64.StdEncoding.EncodeToString(b)
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				startSSE(w)
				for _, d := range deltas {
					writeEvent(t, w, audioEvent(d))
				}
				writeEvent(t, w, "data: [DONE]\n\n")
			}))
			defer srv.Close()

			sr, err := New(WithAPIKey("k"), WithBaseURL(srv.URL)).DoStream(context.Background(), sdk.SpeechParams{Text: "hi"})
			if err != nil {
				t.Fatalf("DoStream: %v", err)
			}
			chunks := drain(t, sr.Stream)
			if err := sr.Err(); err != nil {
				t.Fatalf("Err: %v", err)
			}
			got := checkStreamedWAV(t, chunks)
			if !bytes.Equal(got, want) {
				t.Fatalf("streamed PCM = % x, want % x", got, want)
			}
			if synth := synthesizePCM(t, deltas); !bytes.Equal(got, synth) {
				t.Fatalf("streamed PCM = % x, DoSynthesize PCM = % x", got, synth)
			}
		})
	}
}

func TestPCMDecoderSplitBase64(t *testing.T) {
	t.Parallel()
	pcm := pcmBytes(301, 9)
	whole := base64.StdEncoding.EncodeToString(pcm)
	padded := func(b []byte) string { return base64.StdEncoding.EncodeToString(b) }
	cases := map[string][]string{
		// One base64 text cut at arbitrary characters; the first piece leaves
		// one character over, so it cannot be decoded on its own.
		"cut text": {whole[:5], whole[5:6], whole[6:131], whole[131:]},
		// Independently encoded deltas, padded and not.
		"independent": {padded(pcm[:7]), base64.RawStdEncoding.EncodeToString(pcm[7:20]), padded(pcm[20:])},
		// A cut text whose end is followed by independently padded deltas.
		"cut then independent": {whole[:9], whole[9:], padded(pcm[:2]), padded(pcm[2:5])},
	}
	wants := map[string][]byte{
		"cut text":             pcm,
		"independent":          pcm,
		"cut then independent": append(append(append([]byte(nil), pcm...), pcm[:2]...), pcm[2:5]...),
	}
	for name, deltas := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var d pcmDecoder
			var got []byte
			for i, delta := range deltas {
				out, err := d.write(delta)
				if err != nil {
					t.Fatalf("write delta %d: %v", i, err)
				}
				if len(out)%2 != 0 {
					t.Errorf("delta %d yielded %d bytes, not whole samples", i, len(out))
				}
				got = append(got, out...)
			}
			tail, err := d.flush()
			if err != nil {
				t.Fatalf("flush: %v", err)
			}
			got = append(got, tail...)
			if !bytes.Equal(got, wants[name]) {
				t.Fatalf("decoded % x, want % x", got, wants[name])
			}
		})
	}
}

func TestPCMDecoderInvalidBase64(t *testing.T) {
	t.Parallel()
	var d pcmDecoder
	if _, err := d.write("!!!!!"); err == nil {
		t.Fatal("want an error for invalid base64")
	}
}

// midStreamError is the example mid-stream error event from
// https://openrouter.ai/docs/api-reference/errors.
const midStreamError = `data: {"id":"gen-abc123","object":"chat.completion.chunk","created":1234567890,"model":"openai/gpt-4o","provider":"OpenAI","error":{"code":429,"message":"Rate limit exceeded","metadata":{"error_type":"rate_limit_exceeded"}},"choices":[{"index":0,"delta":{"content":""},"finish_reason":"error"}]}` + "\n\n"

func TestMidStreamErrorIsAPIError(t *testing.T) {
	t.Parallel()
	audio := pcmBytes(64, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		startSSE(w)
		writeEvent(t, w, audioEvent(base64.StdEncoding.EncodeToString(audio)))
		writeEvent(t, w, midStreamError)
	}))
	defer srv.Close()
	p := New(WithAPIKey(secretKey), WithBaseURL(srv.URL))

	check := func(t *testing.T, err error) {
		t.Helper()
		apiErr := providertest.WantAPIError(t, err, providerName, 0, sdk.KindRateLimited)
		if apiErr.Type != "rate_limit_exceeded" || apiErr.Code != "429" || apiErr.Message != "Rate limit exceeded" {
			t.Errorf("Type, Code, Message = %q, %q, %q", apiErr.Type, apiErr.Code, apiErr.Message)
		}
		if strings.Contains(err.Error(), secretKey) {
			t.Errorf("error text %q leaks the key", err.Error())
		}
	}

	t.Run("DoStream", func(t *testing.T) {
		sr, err := p.DoStream(context.Background(), sdk.SpeechParams{Text: "hi"})
		if err != nil {
			t.Fatalf("DoStream: %v", err)
		}
		chunks := drain(t, sr.Stream)
		if got := checkStreamedWAV(t, chunks); !bytes.Equal(got, audio) {
			t.Error("audio before the error was not delivered")
		}
		check(t, sr.Err())
	})
	t.Run("DoSynthesize", func(t *testing.T) {
		res, err := p.DoSynthesize(context.Background(), sdk.SpeechParams{Text: "hi"})
		if res != nil {
			t.Error("DoSynthesize returned a result with the error")
		}
		check(t, err)
	})
}

func TestDoStreamHTTPErrorBeforeStream(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = fmt.Fprint(w, `{"error":{"code":403,"message":"The request is prohibited due to a violation of provider Terms Of Service."}}`)
	}))
	defer srv.Close()

	sr, err := New(WithAPIKey("k"), WithBaseURL(srv.URL)).DoStream(context.Background(), sdk.SpeechParams{Text: "hi"})
	if sr != nil {
		t.Error("DoStream returned a stream with the error")
	}
	apiErr := providertest.WantAPIError(t, err, providerName, http.StatusForbidden, sdk.KindPermissionDenied)
	if apiErr.Code != "403" {
		t.Errorf("Code = %q, want 403", apiErr.Code)
	}
}

func TestDoStreamNoAudio(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		startSSE(w)
		writeEvent(t, w, "data: {\"choices\":[{\"delta\":{\"content\":\"\"}}]}\n\n")
		writeEvent(t, w, "data: [DONE]\n\n")
	}))
	defer srv.Close()

	sr, err := New(WithAPIKey("k"), WithBaseURL(srv.URL)).DoStream(context.Background(), sdk.SpeechParams{Text: "hi"})
	if err != nil {
		t.Fatalf("DoStream: %v", err)
	}
	if chunks := drain(t, sr.Stream); len(chunks) != 0 {
		t.Errorf("got %d chunks, want none", len(chunks))
	}
	if err := sr.Err(); !errors.Is(err, errNoAudio) {
		t.Fatalf("Err = %v, want errNoAudio", err)
	}
}

func TestDoStreamContextCancel(t *testing.T) {
	t.Parallel()
	// The server sends more deltas than the stream buffers, then holds the
	// response open, so the producer is blocked either on a send or on a read
	// when ctx is cancelled.
	cases := map[string]struct {
		deltas   int
		readHead bool
	}{
		"while reading upstream": {deltas: 1, readHead: true},
		"while consumer is idle": {deltas: 32, readHead: false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			sent := make(chan struct{})
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				startSSE(w)
				for i := range tc.deltas {
					writeEvent(t, w, audioEvent(base64.StdEncoding.EncodeToString(pcmBytes(64, i))))
				}
				close(sent)
				select {
				case <-r.Context().Done():
				case <-time.After(streamTimeout):
				}
			}))
			defer srv.Close()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			sr, err := New(WithAPIKey("k"), WithBaseURL(srv.URL)).DoStream(ctx, sdk.SpeechParams{Text: "hi"})
			if err != nil {
				t.Fatalf("DoStream: %v", err)
			}
			if tc.readHead {
				if _, ok := receive(t, sr.Stream); !ok {
					t.Fatal("stream closed before the first chunk")
				}
			}
			select {
			case <-sent:
			case <-time.After(streamTimeout):
				t.Fatal("server did not send its deltas")
			}
			cancel()

			drain(t, sr.Stream)
			if err := sr.Err(); !errors.Is(err, context.Canceled) {
				t.Fatalf("Err = %v, want context.Canceled", err)
			}
		})
	}
}
