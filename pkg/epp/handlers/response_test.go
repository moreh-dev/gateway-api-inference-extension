/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package handlers

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"

	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/common/observability/logging"
	fwkdl "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/datalayer"
	fwkrq "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/requestcontrol"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/metadata"
)

const (
	body = `
	{
		"id": "cmpl-573498d260f2423f9e42817bbba3743a",
		"object": "text_completion",
		"created": 1732563765,
		"model": "meta-llama/Llama-3.1-8B-Instruct",
		"choices": [
			{
				"index": 0,
				"text": " Chronicle\nThe San Francisco Chronicle has a new book review section, and it's a good one. The reviews are short, but they're well-written and well-informed. The Chronicle's book review section is a good place to start if you're looking for a good book review.\nThe Chronicle's book review section is a good place to start if you're looking for a good book review. The Chronicle's book review section",
				"logprobs": null,
				"finish_reason": "length",
				"stop_reason": null,
				"prompt_logprobs": null
			}
		],
		"usage": {
			"prompt_tokens": 11,
			"total_tokens": 111,
			"completion_tokens": 100
		}
	}
	`
	bodyWithCachedTokens = `
	{
		"id": "cmpl-573498d260f2423f9e42817bbba3743a",
		"object": "text_completion",
		"created": 1732563765,
		"model": "meta-llama/Llama-3.1-8B-Instruct",
		"choices": [
			{
				"index": 0,
				"text": " Chronicle\nThe San Francisco Chronicle has a new book review section, and it's a good one. The reviews are short, but they're well-written and well-informed. The Chronicle's book review section is a good place to start if you're looking for a good book review.\nThe Chronicle's book review section is a good place to start if you're looking for a good book review. The Chronicle's book review section",
				"logprobs": null,
				"finish_reason": "length",
				"stop_reason": null,
				"prompt_logprobs": null
			}
		],
		"usage": {
			"prompt_tokens": 11,
			"total_tokens": 111,
			"completion_tokens": 100,
			"prompt_token_details": {
				"cached_tokens": 10
			}
		}
	}
	`

	streamingBodyWithoutUsage = `data: {"id":"cmpl-41764c93-f9d2-4f31-be08-3ba04fa25394","object":"text_completion","created":1740002445,"model":"food-review-0","choices":[],"usage":null}
	`

	streamingBodyWithUsage = `data: {"id":"cmpl-41764c93-f9d2-4f31-be08-3ba04fa25394","object":"text_completion","created":1740002445,"model":"food-review-0","choices":[],"usage":{"prompt_tokens":7,"total_tokens":17,"completion_tokens":10}}
data: [DONE]
	`
	streamingBodyWithUsageAndCachedTokens = `data: {"id":"cmpl-41764c93-f9d2-4f31-be08-3ba04fa25394","object":"text_completion","created":1740002445,"model":"food-review-0","choices":[],"usage":{"prompt_tokens":7,"total_tokens":17,"completion_tokens":10,"prompt_token_details":{"cached_tokens":5}}}
data: [DONE]
	`

	// Responses API SSE format test data
	responsesStreamingTokenDelta = "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"Hello\"}\n"

	responsesStreamingReasoningDelta = "event: response.reasoning_text.delta\ndata: {\"type\":\"response.reasoning_text.delta\",\"delta\":\"thinking...\"}\n"

	responsesStreamingCompleted = "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"object\":\"response\",\"usage\":{\"input_tokens\":10,\"output_tokens\":20,\"total_tokens\":30}}}\n"

	responsesStreamingCompletedNoUsage = "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"object\":\"response\"}}\n"
)

type mockDirector struct{}

func (m *mockDirector) HandleResponseBodyStreaming(ctx context.Context, reqCtx *RequestContext) (*RequestContext, error) {
	return reqCtx, nil
}
func (m *mockDirector) HandleResponseBodyComplete(ctx context.Context, reqCtx *RequestContext) (*RequestContext, error) {
	return reqCtx, nil
}
func (m *mockDirector) HandleResponseReceived(ctx context.Context, reqCtx *RequestContext) (*RequestContext, error) {
	return reqCtx, nil
}
func (m *mockDirector) HandlePreRequest(ctx context.Context, reqCtx *RequestContext) (*RequestContext, error) {
	return reqCtx, nil
}
func (m *mockDirector) GetRandomEndpoint() *fwkdl.EndpointMetadata {
	return &fwkdl.EndpointMetadata{}
}
func (m *mockDirector) HandleRequest(ctx context.Context, reqCtx *RequestContext) (*RequestContext, error) {
	return reqCtx, nil
}

func TestHandleResponseBody(t *testing.T) {
	ctx := logutil.NewTestLoggerIntoContext(context.Background())

	tests := []struct {
		name    string
		body    []byte
		reqCtx  *RequestContext
		want    fwkrq.Usage
		wantErr bool
	}{
		{
			name: "success",
			body: []byte(body),
			want: fwkrq.Usage{
				PromptTokens:     11,
				TotalTokens:      111,
				CompletionTokens: 100,
			},
		},
		{
			name: "success with cached tokens",
			body: []byte(bodyWithCachedTokens),
			want: fwkrq.Usage{
				PromptTokens:     11,
				TotalTokens:      111,
				CompletionTokens: 100,
				PromptTokenDetails: &fwkrq.PromptTokenDetails{
					CachedTokens: 10,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := &StreamingServer{}
			server.director = &mockDirector{}
			reqCtx := test.reqCtx
			if reqCtx == nil {
				reqCtx = &RequestContext{}
			}
			_, err := server.HandleResponseBody(ctx, reqCtx, test.body)
			if err != nil {
				if !test.wantErr {
					t.Fatalf("HandleResponseBody returned unexpected error: %v, want %v", err, test.wantErr)
				}
				return
			}

			if diff := cmp.Diff(test.want, reqCtx.Usage); diff != "" {
				t.Errorf("HandleResponseBody returned unexpected response, diff(-want, +got): %v", diff)
			}
		})
	}
}

func TestHandleStreamedResponseBody(t *testing.T) {
	ctx := logutil.NewTestLoggerIntoContext(context.Background())
	tests := []struct {
		name    string
		body    []byte
		reqCtx  *RequestContext
		want    fwkrq.Usage
		wantErr bool
	}{
		{
			name: "streaming request without usage",
			body: []byte(streamingBodyWithoutUsage),
			reqCtx: &RequestContext{
				modelServerStreaming: true,
			},
			wantErr: false,
			// In the middle of streaming response, so request context response is not set yet.
		},
		{
			name: "streaming request with usage",
			body: []byte(streamingBodyWithUsage),
			reqCtx: &RequestContext{
				modelServerStreaming: true,
			},
			wantErr: false,
			want: fwkrq.Usage{
				PromptTokens:     7,
				TotalTokens:      17,
				CompletionTokens: 10,
			},
		},
		{
			name: "streaming request with usage and cached tokens",
			body: []byte(streamingBodyWithUsageAndCachedTokens),
			reqCtx: &RequestContext{
				modelServerStreaming: true,
			},
			wantErr: false,
			want: fwkrq.Usage{
				PromptTokens:     7,
				TotalTokens:      17,
				CompletionTokens: 10,
				PromptTokenDetails: &fwkrq.PromptTokenDetails{
					CachedTokens: 5,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := &StreamingServer{}
			server.director = &mockDirector{}
			reqCtx := test.reqCtx
			if reqCtx == nil {
				reqCtx = &RequestContext{}
			}
			server.HandleResponseBodyModelStreaming(ctx, reqCtx, test.body)

			if diff := cmp.Diff(test.want, reqCtx.Usage); diff != "" {
				t.Errorf("HandleResponseBody returned unexpected response, diff(-want, +got): %v", diff)
			}
		})
	}
}

func TestHandleResponseBodyModelStreaming_TokenAccumulation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		chunks    [][]byte
		wantUsage fwkrq.Usage
	}{
		{
			name: "Standard: Usage and DONE in same chunk",
			chunks: [][]byte{
				[]byte(`data: {"usage":{"prompt_tokens":5,"completion_tokens":10,"total_tokens":15}}` + "\n" + "data: [DONE]\n"),
			},
			wantUsage: fwkrq.Usage{PromptTokens: 5, CompletionTokens: 10, TotalTokens: 15},
		},
		{
			name: "Split: Usage in Chunk 1, DONE in Chunk 2",
			chunks: [][]byte{
				// Chunk 1: Usage data arrives
				[]byte(`data: {"usage":{"prompt_tokens":5,"completion_tokens":10,"total_tokens":15}}` + "\n"),
				// Chunk 2: Stream termination. Should NOT overwrite the usage from Chunk 1.
				[]byte("data: [DONE]\n"),
			},
			wantUsage: fwkrq.Usage{PromptTokens: 5, CompletionTokens: 10, TotalTokens: 15},
		},
		{
			name: "Fragmented: Content -> Usage -> DONE",
			chunks: [][]byte{
				[]byte(`data: {"choices":[{"text":"Hello"}]}` + "\n"),
				[]byte(`data: {"usage":{"prompt_tokens":5,"completion_tokens":10,"total_tokens":15}}` + "\n"),
				[]byte("data: [DONE]\n"),
			},
			wantUsage: fwkrq.Usage{PromptTokens: 5, CompletionTokens: 10, TotalTokens: 15},
		},
		{
			name: "No Usage Data",
			chunks: [][]byte{
				[]byte(`data: {"choices":[{"text":"Hello"}]}` + "\n"),
				[]byte("data: [DONE]\n"),
			},
			wantUsage: fwkrq.Usage{}, // Zero values
		},
		{
			name: "Responses API: Usage in response.completed event",
			chunks: [][]byte{
				[]byte(responsesStreamingTokenDelta),
				[]byte(responsesStreamingTokenDelta),
				[]byte(responsesStreamingCompleted),
			},
			wantUsage: fwkrq.Usage{PromptTokens: 10, CompletionTokens: 20, TotalTokens: 30},
		},
		{
			name: "Responses API: Split - tokens then completed in separate chunks",
			chunks: [][]byte{
				[]byte(responsesStreamingTokenDelta),
				[]byte(responsesStreamingCompleted),
			},
			wantUsage: fwkrq.Usage{PromptTokens: 10, CompletionTokens: 20, TotalTokens: 30},
		},
		{
			name: "Responses API: No usage in completed event",
			chunks: [][]byte{
				[]byte(responsesStreamingTokenDelta),
				[]byte(responsesStreamingCompletedNoUsage),
			},
			wantUsage: fwkrq.Usage{}, // Zero values
		},
		{
			name: "Responses API: data line split across gRPC chunks",
			chunks: [][]byte{
				[]byte(responsesStreamingTokenDelta),
				// response.completed: event line + partial data in one chunk
				[]byte("event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"object\":"),
				// continuation of data line in next chunk
				[]byte("\"response\",\"usage\":{\"input_tokens\":10,\"output_tokens\":20,\"total_tokens\":30}}}\n"),
			},
			wantUsage: fwkrq.Usage{PromptTokens: 10, CompletionTokens: 20, TotalTokens: 30},
		},
		{
			name: "EndOfStream flush: final chunk has no trailing newline",
			chunks: [][]byte{
				[]byte(responsesStreamingTokenDelta),
				// Final chunk without trailing \n — buffered as partialSSEData,
				// flushed by EndOfStream passing "\n" to trigger processing.
				[]byte("event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"object\":\"response\",\"usage\":{\"input_tokens\":10,\"output_tokens\":20,\"total_tokens\":30}}}"),
			},
			wantUsage: fwkrq.Usage{PromptTokens: 10, CompletionTokens: 20, TotalTokens: 30},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := &StreamingServer{
				director: &mockDirector{},
			}
			reqCtx := &RequestContext{}

			for _, chunk := range tc.chunks {
				server.HandleResponseBodyModelStreaming(context.Background(), reqCtx, chunk)
			}
			// Simulate EndOfStream flush (server.go flushes partialSSEData on EndOfStream).
			// Pass only "\n" — HandleResponseBodyModelStreaming will prepend
			// and clear partialSSEData internally, avoiding double-processing.
			if reqCtx.partialSSEData != "" {
				server.HandleResponseBodyModelStreaming(context.Background(), reqCtx, []byte("\n"))
			}

			assert.Equal(t, tc.wantUsage, reqCtx.Usage, "Usage data should match expected accumulation")
			assert.True(t, reqCtx.ResponseComplete, "Response should be marked complete after [DONE]")
		})
	}
}

func TestHandleResponseBodyModelStreaming_TTFTAndITL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		chunks       [][]byte
		wantTTFTSet  bool
		wantITLCount int
	}{
		{
			name: "TTFT is set on first token chunk",
			chunks: [][]byte{
				[]byte(`data: {"choices":[{"delta":{"content":"Hello"}}]}` + "\n"),
				[]byte(`data: {"choices":[{"delta":{"content":" world"}}]}` + "\n"),
			},
			wantTTFTSet:  true,
			wantITLCount: 1, // one interval between two tokens
		},
		{
			name: "Usage-only events do not set TTFT",
			chunks: [][]byte{
				[]byte(`data: {"choices":[],"usage":{"prompt_tokens":5,"completion_tokens":10,"total_tokens":15}}` + "\n"),
			},
			wantTTFTSet:  false,
			wantITLCount: 0,
		},
		{
			name: "Responses API: TTFT set on output_text.delta",
			chunks: [][]byte{
				[]byte(responsesStreamingTokenDelta),
				[]byte(responsesStreamingTokenDelta),
			},
			wantTTFTSet:  true,
			wantITLCount: 1,
		},
		{
			name: "Responses API: reasoning_text.delta also sets TTFT",
			chunks: [][]byte{
				[]byte(responsesStreamingReasoningDelta),
				[]byte(responsesStreamingTokenDelta),
			},
			wantTTFTSet:  true,
			wantITLCount: 1,
		},
		{
			name: "Responses API: response.completed does not set TTFT",
			chunks: [][]byte{
				[]byte(responsesStreamingCompleted),
			},
			wantTTFTSet:  false,
			wantITLCount: 0,
		},
		{
			name: "Responses API: event: and data: split across chunks",
			chunks: [][]byte{
				// First chunk: event type line only (no corresponding data: line)
				[]byte("event: response.output_text.delta\n"),
				// Second chunk: data: line arrives without preceding event: line
				[]byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"Hello\"}\n"),
				// Third chunk: another token as a single event
				[]byte(responsesStreamingTokenDelta),
			},
			wantTTFTSet:  true,
			wantITLCount: 1, // one interval between two tokens
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := &StreamingServer{
				director: &mockDirector{},
			}
			reqCtx := &RequestContext{}

			for _, chunk := range tc.chunks {
				server.HandleResponseBodyModelStreaming(context.Background(), reqCtx, chunk)
			}

			if tc.wantTTFTSet {
				assert.False(t, reqCtx.FirstTokenTimestamp.IsZero(), "FirstTokenTimestamp should be set")
			} else {
				assert.True(t, reqCtx.FirstTokenTimestamp.IsZero(), "FirstTokenTimestamp should not be set")
			}
			assert.Equal(t, tc.wantITLCount, reqCtx.ITLCount, "ITLCount mismatch")
		})
	}
}

func TestGenerateResponseHeaders_Sanitization(t *testing.T) {
	server := &StreamingServer{}
	reqCtx := &RequestContext{
		Response: &Response{
			Headers: map[string]string{
				"x-backend-server":              "vllm-v0.6.3",            // should passthrough
				metadata.ObjectiveKey:           "sensitive-objective-id", // should be stripped
				metadata.DestinationEndpointKey: "10.2.0.5:8080",          // should be stripped
				"content-length":                "500",                    // hould be stripped
			},
		},
	}

	results := server.generateResponseHeaders(reqCtx)

	gotHeaders := make(map[string]string)
	for _, h := range results {
		gotHeaders[h.Header.Key] = string(h.Header.RawValue)
	}

	assert.Contains(t, gotHeaders, "x-backend-server")
	assert.Contains(t, gotHeaders, "x-went-into-resp-headers")
	assert.NotContains(t, gotHeaders, metadata.ObjectiveKey)
	assert.NotContains(t, gotHeaders, metadata.DestinationEndpointKey)
	assert.NotContains(t, gotHeaders, "content-length")
}
