package sdk

import "context"

// Client carries the embedding, speech, transcription, image and video calls
// as methods; each also exists as a package-level function. Chat calls are
// Model.Generate and Model.Stream (model_call.go) and have no Client form.
type Client struct{}

func NewClient() *Client {
	return &Client{}
}

// --- Embedding convenience functions ---

func Embed(ctx context.Context, value string, options ...EmbedOption) ([]float64, error) {
	return defaultClient.Embed(ctx, value, options...)
}

func EmbedMany(ctx context.Context, values []string, options ...EmbedOption) (*EmbedResult, error) {
	return defaultClient.EmbedMany(ctx, values, options...)
}

// --- Speech convenience functions ---

func GenerateSpeech(ctx context.Context, options ...SpeechOption) (*SpeechResult, error) {
	return defaultClient.GenerateSpeech(ctx, options...)
}

func StreamSpeech(ctx context.Context, options ...SpeechOption) (*SpeechStreamResult, error) {
	return defaultClient.StreamSpeech(ctx, options...)
}

// --- Transcription convenience functions ---

func Transcribe(ctx context.Context, options ...TranscriptionOption) (*TranscriptionResult, error) {
	return defaultClient.Transcribe(ctx, options...)
}

// --- Evaluation convenience functions ---

func Evaluate(ctx context.Context, options ...EvaluateOption) (*EvaluateResult, error) {
	return defaultClient.Evaluate(ctx, options...)
}

// --- Image convenience functions ---

func GenerateImage(ctx context.Context, options ...ImageGenerateOption) (*ImageResult, error) {
	return defaultClient.GenerateImage(ctx, options...)
}

func EditImage(ctx context.Context, options ...ImageEditOption) (*ImageResult, error) {
	return defaultClient.EditImage(ctx, options...)
}

// --- Video convenience functions ---

func CreateVideo(ctx context.Context, options ...VideoOption) (*VideoJob, error) {
	return defaultClient.CreateVideo(ctx, options...)
}

func GetVideo(ctx context.Context, model *VideoModel, id string) (*VideoJob, error) {
	return defaultClient.GetVideo(ctx, model, id)
}

func CancelVideo(ctx context.Context, model *VideoModel, id string) error {
	return defaultClient.CancelVideo(ctx, model, id)
}

func DownloadVideo(ctx context.Context, model *VideoModel, output VideoOutput) (data []byte, contentType string, err error) {
	return defaultClient.DownloadVideo(ctx, model, output)
}

func GenerateVideo(ctx context.Context, options ...VideoOption) (*VideoResult, error) {
	return defaultClient.GenerateVideo(ctx, options...)
}

var defaultClient = &Client{}
