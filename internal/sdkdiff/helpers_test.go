package sdkdiff_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/felinics/twilight/sdk"
)

// reply is one error response: a non-2xx status with a JSON body, or, when
// Events is set, a 200 event stream.
type reply struct {
	Status int
	Header http.Header
	Body   string
	// Events are the name and data of each event of a 200 stream.
	Events [][2]string
}

// serve starts a server that answers every request with r.
func serve(t *testing.T, r reply) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		for name, values := range r.Header {
			w.Header()[name] = values
		}
		if r.Events == nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(r.Status)
			_, _ = w.Write([]byte(r.Body))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		for _, e := range r.Events {
			_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", e[0], e[1])
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func chatRequest(model string) sdk.Request {
	return sdk.Request{Model: model, Messages: []sdk.Message{sdk.UserMessage("hi")}}
}

// twilightGenerate returns the *sdk.APIError of a Generate call.
func twilightGenerate(t *testing.T, p sdk.Provider, model string) *sdk.APIError {
	t.Helper()
	m := &sdk.Model{ID: model, Provider: p}
	_, err := m.Generate(context.Background(), chatRequest(model))
	return asAPIError(t, err)
}

// twilightStream returns the *sdk.APIError of a Stream call's ErrorPart.
func twilightStream(t *testing.T, p sdk.Provider, model string) *sdk.APIError {
	t.Helper()
	m := &sdk.Model{ID: model, Provider: p}
	stream, err := m.Stream(context.Background(), chatRequest(model))
	if err != nil {
		return asAPIError(t, err)
	}
	var failure error
	for part := range stream.Parts {
		if part, ok := part.(*sdk.ErrorPart); ok {
			failure = part.Error
		}
	}
	return asAPIError(t, failure)
}

func asAPIError(t *testing.T, err error) *sdk.APIError {
	t.Helper()
	var apiErr *sdk.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("twilight: error %v (%T) does not unwrap to *sdk.APIError", err, err)
	}
	return apiErr
}

// field is one value read by both SDKs.
type field struct {
	name               string
	twilight, official any
}

func agree(t *testing.T, fields ...field) {
	t.Helper()
	for _, f := range fields {
		if f.twilight != f.official {
			t.Errorf("%s: twilight %v, official SDK %v", f.name, f.twilight, f.official)
		}
	}
}
