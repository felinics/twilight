package speech

// DashScope CosyVoice WebSocket client.
// Reference: https://help.aliyun.com/zh/model-studio/developer-reference/cosyvoice-websocket-api
//
// Protocol flow:
//
//	Client  ── Connect (Authorization: Bearer {api_key}) ──> DashScope Server
//	Client  ── run-task (JSON) ──────────────────────────>  DashScope Server
//	Client  <─ task-started (JSON) ──────────────────────   DashScope Server
//	Client  ── continue-task (JSON, text) ───────────────>  DashScope Server
//	Client  ── finish-task (JSON) ───────────────────────>  DashScope Server
//	Client  <─ result-generated (JSON) + Binary frames ──   DashScope Server
//	Client  <─ task-finished (JSON) ─────────────────────   DashScope Server
//
// Notes:
//   - task_id must remain consistent across run-task, continue-task, and finish-task.
//   - Authorization is set during the WebSocket upgrade handshake.
//   - Binary frames are raw audio bytes with no additional framing.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

const (
	wsKeyStreaming    = "streaming"
	wsKeyHeader       = "header"
	wsKeyAction       = "action"
	wsKeyTaskID       = "task_id"
	wsStreamingDuplex = "duplex"
	wsKeyPayload      = "payload"
	wsKeyInput        = "input"
)

// wsClient manages a single CosyVoice synthesis session over WebSocket.
type wsClient struct {
	apiKey  string
	baseURL string
}

func newWSClient(apiKey, baseURL string) *wsClient {
	return &wsClient{apiKey: apiKey, baseURL: baseURL}
}

// dial opens a new WebSocket connection with Bearer auth.
func (c *wsClient) dial(ctx context.Context) (*websocket.Conn, error) {
	h := http.Header{}
	h.Set("Authorization", "Bearer "+c.apiKey)
	// Required by DashScope to enable data inspection
	h.Set("X-DashScope-DataInspection", "enable")

	conn, resp, err := websocket.DefaultDialer.DialContext(ctx, c.baseURL, h)
	if err != nil {
		if resp != nil {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("alibabacloud speech: ws dial: status=%d: %w", resp.StatusCode, err)
		}
		return nil, fmt.Errorf("alibabacloud speech: ws dial: %w", err)
	}
	return conn, nil
}

// synthesize runs a full CosyVoice synthesis session and returns all audio bytes.
func (c *wsClient) synthesize(ctx context.Context, text string, cfg *audioConfig) ([]byte, error) {
	conn, err := c.dial(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	taskID := uuid.New().String()

	if err := c.sendRunTask(conn, taskID, cfg); err != nil {
		return nil, err
	}

	var audioOut []byte

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		mt, data, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				return audioOut, nil
			}
			return nil, fmt.Errorf("alibabacloud speech: read: %w", err)
		}

		switch mt {
		case websocket.BinaryMessage:
			audioOut = append(audioOut, data...)

		case websocket.TextMessage:
			event, err := parseEvent(data)
			if err != nil {
				continue
			}
			switch event {
			case "task-started":
				if err := c.sendContinueTask(conn, taskID, text); err != nil {
					return nil, err
				}
				if err := c.sendFinishTask(conn, taskID); err != nil {
					return nil, err
				}

			case "task-finished":
				return audioOut, nil

			case "task-failed":
				return nil, fmt.Errorf("alibabacloud speech: task-failed: %s", string(data))
			}
		}
	}
}

// stream runs a CosyVoice synthesis session, pushing audio chunks to the returned channel.
func (c *wsClient) stream(ctx context.Context, text string, cfg *audioConfig) (ch <-chan []byte, errCh <-chan error) {
	dataCh := make(chan []byte, 8)
	errChan := make(chan error, 1)
	ch = dataCh
	errCh = errChan

	go func() {
		defer close(dataCh)
		defer close(errChan)

		conn, err := c.dial(ctx)
		if err != nil {
			errChan <- err
			return
		}
		defer conn.Close()

		taskID := uuid.New().String()

		if err := c.sendRunTask(conn, taskID, cfg); err != nil {
			errChan <- err
			return
		}

		for {
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			default:
			}

			mt, data, readErr := conn.ReadMessage()
			if readErr != nil {
				if !websocket.IsCloseError(readErr, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					errChan <- fmt.Errorf("alibabacloud speech: read: %w", readErr)
				}
				return
			}

			switch mt {
			case websocket.BinaryMessage:
				if len(data) > 0 {
					select {
					case dataCh <- data:
					case <-ctx.Done():
						errChan <- ctx.Err()
						return
					}
				}

			case websocket.TextMessage:
				event, err := parseEvent(data)
				if err != nil {
					continue
				}
				switch event {
				case "task-started":
					if err := c.sendContinueTask(conn, taskID, text); err != nil {
						errChan <- err
						return
					}
					if err := c.sendFinishTask(conn, taskID); err != nil {
						errChan <- err
						return
					}

				case "task-finished":
					return

				case "task-failed":
					errChan <- fmt.Errorf("alibabacloud speech: task-failed: %s", string(data))
					return
				}
			}
		}
	}()

	return ch, errCh
}

func (c *wsClient) sendRunTask(conn *websocket.Conn, taskID string, cfg *audioConfig) error {
	params := map[string]any{
		"text_type":   "PlainText",
		"format":      cfg.Format,
		"sample_rate": cfg.SampleRate,
	}
	if cfg.Voice != "" {
		params["voice"] = cfg.Voice
	}
	if cfg.Volume != 0 {
		params["volume"] = cfg.Volume
	}
	if cfg.Rate != 0 {
		params["rate"] = cfg.Rate
	}
	if cfg.Pitch != 0 {
		params["pitch"] = cfg.Pitch
	}

	cmd := map[string]any{
		wsKeyHeader: map[string]any{
			wsKeyAction:    "run-task",
			wsKeyTaskID:    taskID,
			wsKeyStreaming: wsStreamingDuplex,
		},
		wsKeyPayload: map[string]any{
			"task_group": "audio",
			"task":       "tts",
			"function":   "SpeechSynthesizer",
			"model":      cfg.Model,
			"parameters": params,
			wsKeyInput:   map[string]any{},
		},
	}
	return writeJSON(conn, cmd)
}

func (c *wsClient) sendContinueTask(conn *websocket.Conn, taskID, text string) error {
	cmd := map[string]any{
		wsKeyHeader: map[string]any{
			wsKeyAction:    "continue-task",
			wsKeyTaskID:    taskID,
			wsKeyStreaming: wsStreamingDuplex,
		},
		wsKeyPayload: map[string]any{
			wsKeyInput: map[string]any{
				"text": text,
			},
		},
	}
	return writeJSON(conn, cmd)
}

func (c *wsClient) sendFinishTask(conn *websocket.Conn, taskID string) error {
	cmd := map[string]any{
		wsKeyHeader: map[string]any{
			wsKeyAction:    "finish-task",
			wsKeyTaskID:    taskID,
			wsKeyStreaming: wsStreamingDuplex,
		},
		wsKeyPayload: map[string]any{
			wsKeyInput: map[string]any{},
		},
	}
	return writeJSON(conn, cmd)
}

// parseEvent extracts the event name from a DashScope server message.
func parseEvent(data []byte) (string, error) {
	var msg struct {
		Header struct {
			Event string `json:"event"`
		} `json:"header"`
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		return "", err
	}
	return msg.Header.Event, nil
}

func writeJSON(conn *websocket.Conn, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("alibabacloud speech: marshal: %w", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		return fmt.Errorf("alibabacloud speech: write: %w", err)
	}
	return nil
}
