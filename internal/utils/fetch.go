package utils

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

type RequestOptions struct {
	Method  string
	BaseURL string
	Path    string
	Headers map[string]string
	Query   map[string]string
	Body    any
	Prepare func(*http.Request) error
}

type APIError struct {
	StatusCode int    `json:"status_code"`
	Status     string `json:"status"`
	Message    string `json:"message"`
	RawBody    []byte `json:"-"`
}

func (e *APIError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("api error %d: %s", e.StatusCode, e.Message)
	}
	return fmt.Sprintf("api error %d: %s", e.StatusCode, e.Status)
}

// Detail returns the error message with the raw response body appended
// when available, useful for diagnosing opaque upstream errors like
// "Provider returned error".
func (e *APIError) Detail() string {
	base := e.Error()
	if len(e.RawBody) == 0 {
		return base
	}
	const maxBody = 1024
	body := string(e.RawBody)
	if len(body) > maxBody {
		body = body[:maxBody] + "...(truncated)"
	}
	return fmt.Sprintf("%s [body: %s]", base, body)
}

func BuildRequest(ctx context.Context, opts *RequestOptions) (*http.Request, error) {
	fullURL, err := buildURL(opts.BaseURL, opts.Path, opts.Query)
	if err != nil {
		return nil, err
	}

	method := opts.Method
	if method == "" {
		if opts.Body != nil {
			method = http.MethodPost
		} else {
			method = http.MethodGet
		}
	}

	var body io.Reader
	if opts.Body != nil {
		data, err := json.Marshal(opts.Body)
		if err != nil {
			return nil, fmt.Errorf("marshal request body: %w", err)
		}
		body = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, fullURL, body)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	for k, v := range opts.Headers {
		req.Header.Set(k, v)
	}

	// The body is always JSON-encoded above, so custom headers must not relabel it.
	if opts.Body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	if opts.Prepare != nil {
		if err := opts.Prepare(req); err != nil {
			return nil, fmt.Errorf("prepare request: %w", err)
		}
	}

	return req, nil
}

// FetchJSON sends a JSON request and decodes the response into type T.
// Non-2xx responses are returned as *APIError.
func FetchJSON[T any](ctx context.Context, client *http.Client, opts *RequestOptions) (*T, error) {
	if opts.Headers == nil {
		opts.Headers = make(map[string]string)
	}
	if _, ok := opts.Headers["Accept"]; !ok {
		opts.Headers["Accept"] = "application/json"
	}

	req, err := BuildRequest(ctx, opts)
	if err != nil {
		return nil, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, parseAPIError(resp)
	}

	var result T
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &result, nil
}

// FetchRaw sends a request and returns the raw *http.Response.
// The caller is responsible for closing the response body.
// Non-2xx responses are returned as *APIError (body already closed).
func FetchRaw(ctx context.Context, client *http.Client, opts *RequestOptions) (*http.Response, error) {
	req, err := BuildRequest(ctx, opts)
	if err != nil {
		return nil, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		return nil, parseAPIError(resp)
	}

	return resp, nil
}

// ProbeStatus sends a request and returns only the HTTP status code.
// The response body is always drained and closed. This is useful for
// lightweight endpoint probes where only reachability matters.
func ProbeStatus(ctx context.Context, client *http.Client, opts *RequestOptions) (int, error) {
	req, err := BuildRequest(ctx, opts)
	if err != nil {
		return 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("request failed: %w", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode, nil
}

// BearerToken returns a formatted Bearer authorization header value.
func BearerToken(token string) string {
	return "Bearer " + token
}

// AuthHeader is a shortcut for creating a headers map with Authorization set.
func AuthHeader(token string) map[string]string {
	return map[string]string{
		"Authorization": BearerToken(token),
	}
}

// BuildURL joins baseURL and path into a full URL string.
func BuildURL(baseURL, path string) (string, error) {
	return buildURLWithQuery(baseURL, path, nil)
}

func buildURL(baseURL, path string, query map[string]string) (string, error) {
	return buildURLWithQuery(baseURL, path, query)
}

func buildURLWithQuery(baseURL, path string, query map[string]string) (string, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("invalid base URL: %w", err)
	}

	if path != "" {
		u = u.JoinPath(path)
	}

	if len(query) > 0 {
		q := u.Query()
		for k, v := range query {
			q.Set(k, v)
		}
		u.RawQuery = q.Encode()
	}

	return u.String(), nil
}

func parseAPIError(resp *http.Response) *APIError {
	body, _ := io.ReadAll(resp.Body)

	apiErr := &APIError{
		StatusCode: resp.StatusCode,
		Status:     resp.Status,
		RawBody:    body,
	}

	var parsed struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
		Message string `json:"message"`
	}
	if json.Unmarshal(body, &parsed) == nil {
		switch {
		case parsed.Error.Message != "":
			apiErr.Message = parsed.Error.Message
		case parsed.Message != "":
			apiErr.Message = parsed.Message
		}
	}

	return apiErr
}
