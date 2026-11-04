// Package files implements Moonshot's Files API for file extraction and
// remote-file lifecycle management.
package files

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strings"

	"github.com/memohai/twilight-ai/internal/utils"
)

const (
	// DefaultBaseURL is the China-region Moonshot API endpoint.
	DefaultBaseURL = "https://api.moonshot.cn/v1"

	// MaxContentBytes bounds the convenience Content method. Callers that need
	// streaming or a different limit can use OpenContent instead.
	MaxContentBytes int64 = 100 * 1024 * 1024
)

// Client calls Moonshot's Files API. It is intentionally independent from the
// Chat Completions provider: file extraction is a stateful upload/read/delete
// workflow, not a native message content part.
type Client struct {
	apiKey     string
	baseURL    string
	httpClient *http.Client
}

// Option configures a Client.
type Option func(*Client)

// WithAPIKey configures the Moonshot API key.
func WithAPIKey(apiKey string) Option {
	return func(c *Client) { c.apiKey = apiKey }
}

// WithBaseURL configures the API base URL.
func WithBaseURL(baseURL string) Option {
	return func(c *Client) { c.baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/") }
}

// WithHTTPClient configures the HTTP client.
func WithHTTPClient(client *http.Client) Option {
	return func(c *Client) { c.httpClient = client }
}

// New creates a Moonshot Files API client.
func New(options ...Option) *Client {
	c := &Client{
		baseURL:    DefaultBaseURL,
		httpClient: &http.Client{},
	}
	for _, option := range options {
		option(c)
	}
	if c.httpClient == nil {
		c.httpClient = &http.Client{}
	}
	return c
}

// Upload sends a file to POST /files. The multipart body is streamed from the
// supplied reader instead of buffering the whole document in memory.
func (c *Client) Upload(ctx context.Context, params UploadParams) (*File, error) {
	if params.Reader == nil {
		return nil, errors.New("moonshot files: reader is required")
	}
	params.Filename = strings.TrimSpace(params.Filename)
	if params.Filename == "" {
		return nil, errors.New("moonshot files: filename is required")
	}
	if strings.IndexFunc(params.Filename, func(r rune) bool { return r < 0x20 || r == 0x7f }) >= 0 {
		return nil, errors.New("moonshot files: filename cannot be encoded in a multipart header")
	}
	params.Purpose = Purpose(strings.TrimSpace(string(params.Purpose)))
	if params.Purpose == "" {
		return nil, errors.New("moonshot files: purpose is required")
	}
	if strings.TrimSpace(params.ContentType) == "" {
		params.ContentType = "application/octet-stream"
	}
	mediaType, mediaTypeParams, err := mime.ParseMediaType(params.ContentType)
	if err != nil {
		return nil, fmt.Errorf("moonshot files: invalid content type: %w", err)
	}
	params.ContentType = mime.FormatMediaType(mediaType, mediaTypeParams)
	disposition := mime.FormatMediaType("form-data", map[string]string{
		"name":     "file",
		"filename": params.Filename,
	})
	if disposition == "" {
		return nil, errors.New("moonshot files: filename cannot be encoded in a multipart header")
	}

	bodyReader, bodyWriter := io.Pipe()
	multipartWriter := multipart.NewWriter(bodyWriter)
	contentType := multipartWriter.FormDataContentType()
	go writeUploadBody(bodyWriter, multipartWriter, params, disposition)

	requestURL, err := utils.BuildURL(c.baseURL, "/files")
	if err != nil {
		_ = bodyReader.Close()
		return nil, fmt.Errorf("moonshot files: build upload URL: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, bodyReader)
	if err != nil {
		_ = bodyReader.Close()
		return nil, fmt.Errorf("moonshot files: build upload request: %w", err)
	}
	req.Header.Set("Authorization", utils.BearerToken(c.apiKey))
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("moonshot files: upload request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, readResponseError("upload", resp)
	}

	var file File
	if err := json.NewDecoder(resp.Body).Decode(&file); err != nil {
		return nil, fmt.Errorf("moonshot files: decode upload response: %w", err)
	}
	return &file, nil
}

// UploadForExtraction uploads a document with purpose=file-extract.
func (c *Client) UploadForExtraction(
	ctx context.Context,
	reader io.Reader,
	filename string,
	contentType string,
) (*File, error) {
	return c.Upload(ctx, UploadParams{
		Reader:      reader,
		Filename:    filename,
		ContentType: contentType,
		Purpose:     PurposeFileExtract,
	})
}

// Retrieve returns metadata for an uploaded file.
func (c *Client) Retrieve(ctx context.Context, fileID string) (*File, error) {
	path, err := filePath(fileID, "")
	if err != nil {
		return nil, err
	}
	file, err := utils.FetchJSON[File](ctx, c.httpClient, &utils.RequestOptions{
		Method:  http.MethodGet,
		BaseURL: c.baseURL,
		Path:    path,
		Headers: utils.AuthHeader(c.apiKey),
	})
	if err != nil {
		return nil, fmt.Errorf("moonshot files: retrieve request failed: %w", err)
	}
	return file, nil
}

// OpenContent opens the extracted text returned by
// GET /files/{file_id}/content. The caller must close the returned reader.
func (c *Client) OpenContent(ctx context.Context, fileID string) (io.ReadCloser, error) {
	path, err := filePath(fileID, "/content")
	if err != nil {
		return nil, err
	}
	resp, err := utils.FetchRaw(ctx, c.httpClient, &utils.RequestOptions{
		Method:  http.MethodGet,
		BaseURL: c.baseURL,
		Path:    path,
		Headers: map[string]string{
			"Authorization": utils.BearerToken(c.apiKey),
			"Accept":        "text/plain",
		},
	})
	if err != nil {
		return nil, fmt.Errorf("moonshot files: content request failed: %w", err)
	}
	return resp.Body, nil
}

// Content returns the extracted file content as text. It rejects responses
// larger than MaxContentBytes; use OpenContent when the caller owns streaming
// and its own limit.
func (c *Client) Content(ctx context.Context, fileID string) (string, error) {
	reader, err := c.OpenContent(ctx, fileID)
	if err != nil {
		return "", err
	}
	defer reader.Close()

	data, err := io.ReadAll(io.LimitReader(reader, MaxContentBytes+1))
	if err != nil {
		return "", fmt.Errorf("moonshot files: read content response: %w", err)
	}
	if int64(len(data)) > MaxContentBytes {
		return "", fmt.Errorf("moonshot files: content response exceeds %d bytes", MaxContentBytes)
	}
	return string(data), nil
}

// Delete permanently deletes an uploaded file.
func (c *Client) Delete(ctx context.Context, fileID string) (*DeleteResult, error) {
	path, err := filePath(fileID, "")
	if err != nil {
		return nil, err
	}
	result, err := utils.FetchJSON[DeleteResult](ctx, c.httpClient, &utils.RequestOptions{
		Method:  http.MethodDelete,
		BaseURL: c.baseURL,
		Path:    path,
		Headers: utils.AuthHeader(c.apiKey),
	})
	if err != nil {
		return nil, fmt.Errorf("moonshot files: delete request failed: %w", err)
	}
	return result, nil
}

func writeUploadBody(
	pipeWriter *io.PipeWriter,
	writer *multipart.Writer,
	params UploadParams,
	disposition string,
) {
	var writeErr error
	defer func() {
		if closeErr := writer.Close(); writeErr == nil {
			writeErr = closeErr
		}
		_ = pipeWriter.CloseWithError(writeErr)
	}()

	if err := writer.WriteField("purpose", string(params.Purpose)); err != nil {
		writeErr = err
		return
	}
	header := make(textproto.MIMEHeader)
	header.Set("Content-Disposition", disposition)
	header.Set("Content-Type", params.ContentType)
	part, err := writer.CreatePart(header)
	if err != nil {
		writeErr = err
		return
	}
	_, writeErr = io.Copy(part, params.Reader)
}

func filePath(fileID, suffix string) (string, error) {
	fileID = strings.TrimSpace(fileID)
	if fileID == "" {
		return "", errors.New("moonshot files: file id is required")
	}
	if strings.ContainsAny(fileID, "/?#") {
		return "", errors.New("moonshot files: file id contains path separators")
	}
	return "/files/" + fileID + suffix, nil
}

func readResponseError(operation string, resp *http.Response) error {
	const maxErrorBytes = 16 * 1024
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBytes))
	message := strings.TrimSpace(string(body))
	var envelope struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &envelope) == nil && strings.TrimSpace(envelope.Error.Message) != "" {
		message = strings.TrimSpace(envelope.Error.Message)
	}
	if message == "" {
		message = http.StatusText(resp.StatusCode)
	}
	return fmt.Errorf("moonshot files: %s failed with status %d: %s", operation, resp.StatusCode, message)
}
