package files

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestUploadForExtractionStreamsMultipartFile(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/files" {
			t.Fatalf("request = %s %s, want POST /v1/files", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer secret" {
			t.Fatalf("Authorization = %q", got)
		}
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Fatalf("ParseMultipartForm: %v", err)
		}
		if got := r.FormValue("purpose"); got != "file-extract" {
			t.Fatalf("purpose = %q", got)
		}
		file, header, err := r.FormFile("file")
		if err != nil {
			t.Fatalf("FormFile: %v", err)
		}
		defer file.Close()
		if header.Filename != "report.pdf" {
			t.Fatalf("filename = %q", header.Filename)
		}
		if got := header.Header.Get("Content-Type"); got != "application/pdf" {
			t.Fatalf("file Content-Type = %q", got)
		}
		data, err := io.ReadAll(file)
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if got := string(data); got != "%PDF-test" {
			t.Fatalf("file data = %q", got)
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"file-1","object":"file","bytes":9,"created_at":123,"filename":"report.pdf","purpose":"file-extract","status":"ready","status_details":""}`))
	}))
	defer server.Close()

	client := New(WithAPIKey("secret"), WithBaseURL(server.URL+"/v1/"))
	file, err := client.UploadForExtraction(
		context.Background(),
		strings.NewReader("%PDF-test"),
		"report.pdf",
		"application/pdf",
	)
	if err != nil {
		t.Fatalf("UploadForExtraction: %v", err)
	}
	if file.ID != "file-1" || file.Purpose != PurposeFileExtract || file.Status != "ready" {
		t.Fatalf("file = %#v", file)
	}
}

func TestRetrieveContentAndDelete(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer secret" {
			t.Fatalf("Authorization = %q", got)
		}
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/files/file-1":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"file-1","object":"file","filename":"report.pdf","purpose":"file-extract","status":"ready"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/files/file-1/content":
			if got := r.Header.Get("Accept"); got != "text/plain" {
				t.Fatalf("Accept = %q", got)
			}
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte("extracted document text"))
		case r.Method == http.MethodDelete && r.URL.Path == "/files/file-1":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"file-1","object":"file","deleted":true}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	client := New(WithAPIKey("secret"), WithBaseURL(server.URL))
	file, err := client.Retrieve(context.Background(), "file-1")
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	if file.ID != "file-1" || file.Filename != "report.pdf" {
		t.Fatalf("file = %#v", file)
	}

	content, err := client.Content(context.Background(), "file-1")
	if err != nil {
		t.Fatalf("Content: %v", err)
	}
	if content != "extracted document text" {
		t.Fatalf("content = %q", content)
	}

	deleted, err := client.Delete(context.Background(), "file-1")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !deleted.Deleted || deleted.ID != "file-1" {
		t.Fatalf("delete result = %#v", deleted)
	}
}

func TestUploadReturnsMoonshotErrorMessage(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"message":"unsupported file type","type":"invalid_request_error"}}`))
	}))
	defer server.Close()

	client := New(WithBaseURL(server.URL))
	_, err := client.UploadForExtraction(
		context.Background(),
		strings.NewReader("data"),
		"archive.zip",
		"application/zip",
	)
	if err == nil || !strings.Contains(err.Error(), "status 400: unsupported file type") {
		t.Fatalf("error = %v", err)
	}
}

func TestInputValidation(t *testing.T) {
	t.Parallel()

	client := New(WithHTTPClient(nil))
	tests := []struct {
		name string
		call func() error
		want string
	}{
		{
			name: "reader",
			call: func() error {
				_, err := client.Upload(context.Background(), UploadParams{Filename: "a.pdf", Purpose: PurposeFileExtract})
				return err
			},
			want: "reader is required",
		},
		{
			name: "filename",
			call: func() error {
				_, err := client.Upload(context.Background(), UploadParams{Reader: strings.NewReader("x"), Purpose: PurposeFileExtract})
				return err
			},
			want: "filename is required",
		},
		{
			name: "filename header",
			call: func() error {
				_, err := client.Upload(context.Background(), UploadParams{
					Reader:   strings.NewReader("x"),
					Filename: "a.pdf\r\nX-Injected: true",
					Purpose:  PurposeFileExtract,
				})
				return err
			},
			want: "filename cannot be encoded",
		},
		{
			name: "purpose",
			call: func() error {
				_, err := client.Upload(context.Background(), UploadParams{Reader: strings.NewReader("x"), Filename: "a.pdf"})
				return err
			},
			want: "purpose is required",
		},
		{
			name: "content type",
			call: func() error {
				_, err := client.Upload(context.Background(), UploadParams{
					Reader:      strings.NewReader("x"),
					Filename:    "a.pdf",
					ContentType: "application/pdf\r\nX-Injected: true",
					Purpose:     PurposeFileExtract,
				})
				return err
			},
			want: "invalid content type",
		},
		{
			name: "file id",
			call: func() error {
				_, err := client.Content(context.Background(), "")
				return err
			},
			want: "file id is required",
		},
		{
			name: "file id separator",
			call: func() error {
				_, err := client.Delete(context.Background(), "../file")
				return err
			},
			want: "path separators",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.call()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}
