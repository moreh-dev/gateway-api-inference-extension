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
	"encoding/json"
	"strings"
	"time"

	configPb "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/gateway-api-inference-extension/pkg/common"
	reqenvoy "sigs.k8s.io/gateway-api-inference-extension/pkg/common/envoy/request"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/common/observability/logging"
	fwkrq "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/requestcontrol"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/metrics"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/request"
)

const (
	streamingRespPrefix = "data: "
	streamingEndMsg     = "data: [DONE]"

	// SSE event prefix and Responses API event types.
	// Responses API uses paired "event: <type>\ndata: <json>" lines,
	// unlike Chat Completions which uses only "data: <json>" lines.
	sseEventPrefix              = "event: "
	responsesOutputTextDelta    = "response.output_text.delta"
	responsesReasoningTextDelta = "response.reasoning_text.delta"
	responsesCompleted          = "response.completed"

	// OpenAI API object types
	objectTypeResponse            = "response"
	objectTypeConversation        = "conversation"
	objectTypeChatCompletion      = "chat.completion"
	objectTypeChatCompletionChunk = "chat.completion.chunk"
	objectTypeTextCompletion      = "text_completion"
)

// extractUsageByAPIType extracts usage statistics using the appropriate field names
// based on the OpenAI API type identified by the "object" field.
func extractUsageByAPIType(usg map[string]any, objectType string) fwkrq.Usage {
	usage := fwkrq.Usage{}

	switch {
	case strings.HasPrefix(objectType, objectTypeResponse) || strings.HasPrefix(objectType, objectTypeConversation):
		// Responses/Conversations APIs use input_tokens/output_tokens
		if usg["input_tokens"] != nil {
			usage.PromptTokens = int(usg["input_tokens"].(float64))
		}
		if usg["output_tokens"] != nil {
			usage.CompletionTokens = int(usg["output_tokens"].(float64))
		}
	case objectType == objectTypeChatCompletion || objectType == objectTypeChatCompletionChunk || objectType == objectTypeTextCompletion:
		// Traditional APIs use prompt_tokens/completion_tokens
		if usg["prompt_tokens"] != nil {
			usage.PromptTokens = int(usg["prompt_tokens"].(float64))
		}
		if usg["completion_tokens"] != nil {
			usage.CompletionTokens = int(usg["completion_tokens"].(float64))
		}
	default:
		// Fallback: try both field naming conventions
		if usg["input_tokens"] != nil {
			usage.PromptTokens = int(usg["input_tokens"].(float64))
		} else if usg["prompt_tokens"] != nil {
			usage.PromptTokens = int(usg["prompt_tokens"].(float64))
		}

		if usg["output_tokens"] != nil {
			usage.CompletionTokens = int(usg["output_tokens"].(float64))
		} else if usg["completion_tokens"] != nil {
			usage.CompletionTokens = int(usg["completion_tokens"].(float64))
		}
	}

	// total_tokens field name is consistent across all API types
	if usg["total_tokens"] != nil {
		usage.TotalTokens = int(usg["total_tokens"].(float64))
	}

	return usage
}

// HandleResponseBody always returns the requestContext even in the error case, as the request context is used in error handling.
func (s *StreamingServer) HandleResponseBody(ctx context.Context, reqCtx *RequestContext, responseBytes []byte) (*RequestContext, error) {
	logger := log.FromContext(ctx)
	var responseErr error
	var responseBody map[string]any
	responseErr = json.Unmarshal(responseBytes, &responseBody)
	if responseErr != nil {
		if logger.V(logutil.DEBUG).Enabled() {
			logger.V(logutil.DEBUG).Error(responseErr, "Error unmarshalling request body", "body", string(responseBytes))
		} else {
			logger.V(logutil.DEFAULT).Error(responseErr, "Error unmarshalling request body", "body", string(responseBytes))
		}
		return reqCtx, responseErr
	}

	if responseBody["usage"] != nil {
		usg := responseBody["usage"].(map[string]any)
		objectType, _ := responseBody["object"].(string)
		usage := extractUsageByAPIType(usg, objectType)
		if usg["prompt_token_details"] != nil {
			detailsMap := usg["prompt_token_details"].(map[string]any)
			if cachedTokens, ok := detailsMap["cached_tokens"]; ok {
				usage.PromptTokenDetails = &fwkrq.PromptTokenDetails{
					CachedTokens: int(cachedTokens.(float64)),
				}
			}
		}
		reqCtx.Usage = usage
		logger.V(logutil.VERBOSE).Info("Response generated", "usage", reqCtx.Usage)
	}

	return s.director.HandleResponseBodyComplete(ctx, reqCtx)
}

// HandleResponseBodyModelStreaming handles streaming response if the modelServer is streaming.
// It supports two SSE formats:
//   - Chat Completions: "data: {json}\n" lines with "data: [DONE]" termination
//   - Responses API: paired "event: <type>\ndata: {json}\n" lines with "event: response.completed" termination
func (s *StreamingServer) HandleResponseBodyModelStreaming(ctx context.Context, reqCtx *RequestContext, responseBytes []byte) {
	responseText := string(responseBytes)

	// Reassemble partial SSE lines split across gRPC chunk boundaries.
	// A single SSE data line (e.g., Responses API response.completed containing
	// the full response object) can exceed one gRPC message size.
	if reqCtx.partialSSEData != "" {
		responseText = reqCtx.partialSSEData + responseText
		reqCtx.partialSSEData = ""
	}
	if len(responseText) > 0 && !strings.HasSuffix(responseText, "\n") {
		lastNL := strings.LastIndex(responseText, "\n")
		if lastNL >= 0 {
			reqCtx.partialSSEData = responseText[lastNL+1:]
			responseText = responseText[:lastNL+1]
		} else {
			// Entire chunk is a partial line — buffer all and wait for more.
			reqCtx.partialSSEData = responseText
			return
		}
	}

	// Process SSE lines individually — a single gRPC chunk can contain multiple SSE events.
	// Restore pending event type from previous chunk (event: and data: lines may span chunks).
	currentEventType := reqCtx.pendingSSEEventType
	reqCtx.pendingSSEEventType = ""
	for line := range strings.SplitSeq(responseText, "\n") {
		line = strings.TrimSpace(line)

		// Track Responses API event types (event: lines precede their data: lines).
		if strings.HasPrefix(line, sseEventPrefix) {
			currentEventType = strings.TrimPrefix(line, sseEventPrefix)
			continue
		}

		if !strings.HasPrefix(line, streamingRespPrefix) || line == streamingEndMsg {
			continue
		}

		content := strings.TrimPrefix(line, streamingRespPrefix)

		// Determine if this data line represents a token event.
		var isToken bool
		if currentEventType != "" {
			// Responses API: event type determines token vs non-token.
			isToken = currentEventType == responsesOutputTextDelta || currentEventType == responsesReasoningTextDelta
			currentEventType = "" // consumed
		} else {
			// Chat Completions: inspect JSON payload for non-empty choices array.
			// Also handles Responses API fallback via JSON "type" field when
			// event: and data: lines are split across gRPC chunks.
			isToken = isTokenEvent(content)
		}

		if !isToken {
			continue
		}

		// Capture per-token timestamp so ITL reflects actual inter-token arrival
		// timing even when multiple SSE events arrive in a single gRPC chunk.
		now := time.Now()

		// TTFT: record timestamp of the first token event.
		if reqCtx.FirstTokenTimestamp.IsZero() {
			reqCtx.FirstTokenTimestamp = now
		}

		// ITL (Inter-Token Latency) collection: always measure when possible.
		if !reqCtx.LastTokenTimestamp.IsZero() {
			itl := now.Sub(reqCtx.LastTokenTimestamp).Seconds()
			metrics.RecordRequestITL(ctx, reqCtx.IncomingModelName, reqCtx.TargetModelName, itl)
			reqCtx.ITLCount++
			reqCtx.ITLSum += itl
		}
		reqCtx.LastTokenTimestamp = now
	}
	// Persist unconsumed event type for the next chunk (event: line at end of chunk
	// with its data: line arriving in the next chunk).
	reqCtx.pendingSSEEventType = currentEventType

	logger := log.FromContext(ctx)
	_, err := s.director.HandleResponseBodyStreaming(ctx, reqCtx)
	if err != nil {
		logger.Error(err, "error in HandleResponseBodyStreaming")
	}
	// Parse usage on EVERY chunk to catch split streams (where usage and [DONE] are in different chunks).
	// Chat Completions: top-level "usage" field.
	if resp := parseRespForUsage(ctx, responseText); resp.Usage.TotalTokens > 0 {
		reqCtx.Usage = resp.Usage
	}
	// Responses API: usage nested inside "response.completed" event data.
	if usage := parseResponsesAPIUsage(ctx, responseText); usage.TotalTokens > 0 {
		reqCtx.Usage = usage
	}

	// Stream completion: Chat Completions uses "data: [DONE]", Responses API uses "event: response.completed".
	// Token count metrics are recorded in server.go's EndOfStream block, after all chunks
	// (including buffer flush) have been processed and Usage is finalized.
	if strings.Contains(responseText, streamingEndMsg) ||
		strings.Contains(responseText, sseEventPrefix+responsesCompleted) {
		reqCtx.ResponseComplete = true
	}
}

func (s *StreamingServer) HandleResponseHeaders(ctx context.Context, reqCtx *RequestContext, resp *extProcPb.ProcessingRequest_ResponseHeaders) (*RequestContext, error) {
	for _, header := range resp.ResponseHeaders.Headers.Headers {
		reqCtx.Response.Headers[header.Key] = reqenvoy.GetHeaderValue(header)
	}

	reqCtx, err := s.director.HandleResponseReceived(ctx, reqCtx)

	return reqCtx, err
}

func (s *StreamingServer) generateResponseHeaderResponse(reqCtx *RequestContext) *extProcPb.ProcessingResponse {
	return &extProcPb.ProcessingResponse{
		Response: &extProcPb.ProcessingResponse_ResponseHeaders{
			ResponseHeaders: &extProcPb.HeadersResponse{
				Response: &extProcPb.CommonResponse{
					HeaderMutation: &extProcPb.HeaderMutation{
						SetHeaders: s.generateResponseHeaders(reqCtx),
					},
				},
			},
		},
	}
}

func generateResponseBodyResponses(responseBodyBytes []byte, setEoS bool) []*extProcPb.ProcessingResponse {
	commonResponses := common.BuildChunkedBodyResponses(responseBodyBytes, setEoS)
	responses := make([]*extProcPb.ProcessingResponse, 0, len(commonResponses))
	for _, commonResp := range commonResponses {
		resp := &extProcPb.ProcessingResponse{
			Response: &extProcPb.ProcessingResponse_ResponseBody{
				ResponseBody: &extProcPb.BodyResponse{
					Response: commonResp,
				},
			},
		}
		responses = append(responses, resp)
	}
	return responses
}

func (s *StreamingServer) generateResponseHeaders(reqCtx *RequestContext) []*configPb.HeaderValueOption {
	// can likely refactor these two bespoke headers to be updated in PostDispatch, to centralize logic.
	headers := []*configPb.HeaderValueOption{
		{
			Header: &configPb.HeaderValue{
				// This is for debugging purpose only.
				Key:      "x-went-into-resp-headers",
				RawValue: []byte("true"),
			},
		},
	}

	// Include any non-system-owned headers.
	for key, value := range reqCtx.Response.Headers {
		if request.IsSystemOwnedHeader(key) {
			continue
		}
		headers = append(headers, &configPb.HeaderValueOption{
			Header: &configPb.HeaderValue{
				Key:      key,
				RawValue: []byte(value),
			},
		})
	}
	return headers
}

// Example message if "stream_options": {"include_usage": "true"} is included in the request:
// data: {"id":"...","object":"text_completion","created":1739400043,"model":"small-segment-lora-0","choices":[],
// "usage":{"prompt_tokens":7,"total_tokens":17,"completion_tokens":10}}
//
// data: [DONE]
//
// Noticed that vLLM returns two entries in one response.
// We need to strip the `data:` prefix and next Data: [DONE] from the message to fetch response data.
//
// If include_usage is not included in the request, `data: [DONE]` is returned separately, which
// indicates end of streaming.
func parseRespForUsage(ctx context.Context, responseText string) ResponseBody {
	response := ResponseBody{}
	logger := log.FromContext(ctx)

	lines := strings.SplitSeq(responseText, "\n")
	for line := range lines {
		if !strings.HasPrefix(line, streamingRespPrefix) {
			continue
		}
		content := strings.TrimPrefix(line, streamingRespPrefix)
		if content == "[DONE]" {
			continue
		}

		byteSlice := []byte(content)
		if err := json.Unmarshal(byteSlice, &response); err != nil {
			logger.Error(err, "unmarshaling response body")
			continue
		}
	}

	return response
}

// parseResponsesAPIUsage extracts usage from a Responses API "response.completed" SSE event.
// The event format is:
//
//	event: response.completed
//	data: {"type":"response.completed","response":{"object":"response","usage":{"input_tokens":10,"output_tokens":20,"total_tokens":30}}}
func parseResponsesAPIUsage(ctx context.Context, responseText string) fwkrq.Usage {
	logger := log.FromContext(ctx)
	var currentEventType string

	for line := range strings.SplitSeq(responseText, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, sseEventPrefix) {
			currentEventType = strings.TrimPrefix(line, sseEventPrefix)
			continue
		}
		if !strings.HasPrefix(line, streamingRespPrefix) {
			continue
		}
		content := strings.TrimPrefix(line, streamingRespPrefix)

		// Identify response.completed events via either:
		// 1. Preceding "event: response.completed" line (normal case)
		// 2. JSON "type" field (handles event: and data: lines split across gRPC chunks)
		isCompletedEvent := currentEventType == responsesCompleted
		currentEventType = "" // consumed
		if !isCompletedEvent && !strings.Contains(content, `"type":"response.completed"`) {
			continue
		}

		var event struct {
			Response struct {
				Object string         `json:"object"`
				Usage  map[string]any `json:"usage"`
			} `json:"response"`
		}
		if err := json.Unmarshal([]byte(content), &event); err != nil {
			logger.Error(err, "unmarshaling Responses API completed event")
			continue
		}
		if event.Response.Usage != nil {
			objectType := event.Response.Object
			if objectType == "" {
				objectType = objectTypeResponse
			}
			return extractUsageByAPIType(event.Response.Usage, objectType)
		}
	}
	return fwkrq.Usage{}
}

// isTokenEvent checks if an SSE data payload contains actual generated token content
// isTokenEvent checks if an SSE data payload contains actual generated token content.
// It handles two formats:
//   - Chat Completions: non-empty choices array
//   - Responses API fallback: JSON "type" field matching token delta event types
//     (used when event: and data: lines are split across gRPC chunks)
func isTokenEvent(jsonPayload string) bool {
	var event struct {
		Choices []json.RawMessage `json:"choices"`
		Type    string            `json:"type"`
	}
	if err := json.Unmarshal([]byte(jsonPayload), &event); err != nil {
		return false
	}
	if len(event.Choices) > 0 {
		return true
	}
	// Responses API fallback: detect token events by JSON "type" field
	return event.Type == responsesOutputTextDelta || event.Type == responsesReasoningTextDelta
}

type ResponseBody struct {
	Usage fwkrq.Usage `json:"usage"`
}

type PromptTokenDetails struct {
	CachedTokens int `json:"cached_tokens"`
}
