package files

import "io"

// Purpose controls how Moonshot processes an uploaded file.
type Purpose string

const (
	// PurposeFileExtract asks Moonshot to extract text from the uploaded file.
	PurposeFileExtract Purpose = "file-extract"
	// PurposeImage marks an upload for an image workflow.
	PurposeImage Purpose = "image"
	// PurposeVideo marks an upload for a video workflow.
	PurposeVideo Purpose = "video"
	// PurposeBatch marks an upload for batch processing.
	PurposeBatch Purpose = "batch"
)

// UploadParams describes a multipart file upload.
type UploadParams struct {
	Reader      io.Reader
	Filename    string
	ContentType string
	Purpose     Purpose
}

// File is Moonshot's metadata for an uploaded file.
type File struct {
	ID            string  `json:"id"`
	Object        string  `json:"object"`
	Bytes         int64   `json:"bytes"`
	CreatedAt     int64   `json:"created_at"`
	Filename      string  `json:"filename"`
	Purpose       Purpose `json:"purpose"`
	Status        string  `json:"status"`
	StatusDetails string  `json:"status_details"`
}

// DeleteResult is returned after deleting an uploaded file.
type DeleteResult struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Deleted bool   `json:"deleted"`
}
