package images

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strconv"

	"github.com/felinics/twilight/internal/utils"
	"github.com/felinics/twilight/sdk"
)

// doEditMultipart sends an image edit request as multipart/form-data.
// Used when any ImageInput carries raw file bytes.
func (p *Provider) doEditMultipart(ctx context.Context, params *sdk.ImageEditParams) (*sdk.ImageResult, error) {
	body, contentType, err := buildMultipartBody(params)
	if err != nil {
		return nil, fmt.Errorf("openai images: build multipart body: %w", err)
	}

	fullURL, err := utils.BuildURL(p.baseURL, "/images/edits")
	if err != nil {
		return nil, fmt.Errorf("openai images: build URL: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fullURL, body)
	if err != nil {
		return nil, fmt.Errorf("openai images: create request: %w", err)
	}
	for key, value := range p.requestHeaders(ctx) {
		req.Header.Set(key, value)
	}
	req.Header.Set("Content-Type", contentType)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("openai images: edit request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("openai images: edit request failed with status %d: %s", resp.StatusCode, string(respBody))
	}

	var result imagesResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("openai images: decode response: %w", err)
	}

	return toImageResult(&result), nil
}

func buildMultipartBody(params *sdk.ImageEditParams) (*bytes.Buffer, string, error) {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)

	if err := w.WriteField("model", params.Model.ID); err != nil {
		return nil, "", err
	}
	if err := w.WriteField("prompt", params.Prompt); err != nil {
		return nil, "", err
	}

	for _, img := range params.Images {
		if len(img.Data) == 0 {
			continue
		}
		filename := img.Filename
		if filename == "" {
			filename = "image.png"
		}
		part, err := w.CreateFormFile("image[]", filename)
		if err != nil {
			return nil, "", err
		}
		if _, err := part.Write(img.Data); err != nil {
			return nil, "", err
		}
	}

	if params.Mask != nil && len(params.Mask.Data) > 0 {
		filename := params.Mask.Filename
		if filename == "" {
			filename = "mask.png"
		}
		part, err := w.CreateFormFile("mask", filename)
		if err != nil {
			return nil, "", err
		}
		if _, err := part.Write(params.Mask.Data); err != nil {
			return nil, "", err
		}
	}

	if params.N != nil {
		if err := w.WriteField("n", strconv.Itoa(*params.N)); err != nil {
			return nil, "", err
		}
	}
	if params.Size != "" {
		if err := w.WriteField("size", params.Size); err != nil {
			return nil, "", err
		}
	}
	if params.Quality != "" {
		if err := w.WriteField("quality", params.Quality); err != nil {
			return nil, "", err
		}
	}
	if params.Background != "" {
		if err := w.WriteField("background", params.Background); err != nil {
			return nil, "", err
		}
	}
	if params.OutputFormat != "" {
		if err := w.WriteField("output_format", params.OutputFormat); err != nil {
			return nil, "", err
		}
	}
	if params.OutputCompression != nil {
		if err := w.WriteField("output_compression", strconv.Itoa(*params.OutputCompression)); err != nil {
			return nil, "", err
		}
	}
	if params.InputFidelity != "" {
		if err := w.WriteField("input_fidelity", params.InputFidelity); err != nil {
			return nil, "", err
		}
	}
	if params.Moderation != "" {
		if err := w.WriteField("moderation", params.Moderation); err != nil {
			return nil, "", err
		}
	}
	if params.ResponseFormat != "" {
		if err := w.WriteField("response_format", params.ResponseFormat); err != nil {
			return nil, "", err
		}
	}
	if params.User != "" {
		if err := w.WriteField("user", params.User); err != nil {
			return nil, "", err
		}
	}

	if err := w.Close(); err != nil {
		return nil, "", err
	}

	return &buf, w.FormDataContentType(), nil
}
