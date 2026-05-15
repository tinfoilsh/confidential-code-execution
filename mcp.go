package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
)

var hex64Re = regexp.MustCompile(`^[0-9a-f]{64}$`)

// 32 bytes encoded as base64url with no padding
var base64Url32Re = regexp.MustCompile(`^[A-Za-z0-9_-]{43}$`)

// codeExecMetaKey is the params._meta sub-key the router uses to ship
// the three per-request orchestrator credentials, plus the optional
// uploads manifest:
//
//	"_meta": {
//	  "tinfoil_code_exec": {
//	    "accessToken":        "<64 hex>",
//	    "encryptionKey":      "<43-char base64url>",
//	    "containerAuthToken": "<64 hex>",
//	    "uploads": [
//	      {"fileAccessToken": "<64 hex>", "filename": "report.pdf", "sha256": "<64 hex>"},
//	      ...
//	    ]
//	  }
//	}
//
// uploads is optional. Absent = leave /user-uploads alone; present (even
// empty) = reconcile /user-uploads to exactly match the manifest.
// fileAccessToken is used to lookup from bucket.
// filename is used for the filename
// sha256 is used for fast path check against sandbox environment
const codeExecMetaKey = "tinfoil_code_exec"

type jsonRPCRequest struct {
	JSONRPC string         `json:"jsonrpc"`
	ID      any            `json:"id,omitempty"`
	Method  string         `json:"method"`
	Params  map[string]any `json:"params,omitempty"`
}

type jsonRPCResponse struct {
	JSONRPC string    `json:"jsonrpc"`
	ID      any       `json:"id,omitempty"`
	Result  any       `json:"result,omitempty"`
	Error   *rpcError `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// HandleMCPRequest processes a JSON-RPC 2.0 MCP request.
// Returns (status, response). If response is nil, the request was a notification.
func HandleMCPRequest(ctx context.Context, m *Manager, headers http.Header, req jsonRPCRequest) (int, *jsonRPCResponse) {
	// Notifications have no id
	if req.ID == nil {
		return http.StatusAccepted, nil
	}

	resp := &jsonRPCResponse{JSONRPC: "2.0", ID: req.ID}

	switch req.Method {
	case "initialize":
		resp.Result = map[string]any{
			"protocolVersion": "2024-11-05",
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo": map[string]any{
				"name":    "confidential-code-execution",
				"version": "0.1.0",
			},
		}
		return http.StatusOK, resp

	case "tools/list":
		resp.Result = map[string]any{"tools": Tools}
		return http.StatusOK, resp

	case "tools/call":
		secrets, rpcErr := extractCodeExecSecrets(req.Params)
		if rpcErr != nil {
			resp.Error = rpcErr
			return http.StatusBadRequest, resp
		}
		accessToken := secrets.accessToken
		containerAuthToken := secrets.containerAuthToken
		encryptionKey := secrets.encryptionKey
		bearer := extractBearer(headers.Get("Authorization"))
		if err := m.AuthorizeSession(ctx, bearer); err != nil {
			if errors.Is(err, ErrAuthRequired) {
				resp.Error = &rpcError{Code: -32001, Message: err.Error()}
				return http.StatusUnauthorized, resp
			}
			resp.Error = &rpcError{Code: -32603, Message: "auth check failed: " + err.Error()}
			return http.StatusInternalServerError, resp
		}
		// Stash on ctx. Encryption key + bearer are cached on the container by the manager.
		// AuthToken only exists per request
		ctx = WithCodeExecutionEncryptionKey(ctx, encryptionKey)
		ctx = WithBearer(ctx, bearer)
		ctx = WithContainerAuthToken(ctx, containerAuthToken)

		uploads, uploadsErr := extractUploads(req.Params)
		if uploadsErr != nil {
			resp.Error = uploadsErr
			return http.StatusBadRequest, resp
		}
		// non-nil is valid - no uploads
		if uploads != nil {
			if err := m.SyncUploads(ctx, accessToken, uploads); err != nil {
				resp.Error = &rpcError{Code: -32603, Message: "sync-uploads failed: " + err.Error()}
				return http.StatusInternalServerError, resp
			}
		}

		name, _ := req.Params["name"].(string)
		args, _ := req.Params["arguments"].(map[string]any)
		handler, ok := ToolHandlers[name]
		if !ok {
			resp.Error = &rpcError{Code: -32601, Message: "unknown tool: " + name}
			return http.StatusBadRequest, resp
		}
		text, err := handler(ctx, m, accessToken, args)
		if err != nil {
			resp.Result = map[string]any{
				"content": []map[string]any{{"type": "text", "text": "Error: " + err.Error()}},
				"isError": true,
			}
		} else {
			resp.Result = map[string]any{
				"content": []map[string]any{{"type": "text", "text": text}},
			}
		}
		return http.StatusOK, resp

	default:
		resp.Error = &rpcError{Code: -32601, Message: "method not found: " + req.Method}
		return http.StatusBadRequest, resp
	}
}

type codeExecSecrets struct {
	accessToken        string
	containerAuthToken string
	encryptionKey      string
}

// extractCodeExecSecrets pulls the three per-request orchestrator
// credentials out of params._meta and validates each one's wire format.
// Returns a populated codeExecSecrets value on success, or an
// MCP-shaped rpcError describing the first failure.
func extractCodeExecSecrets(params map[string]any) (codeExecSecrets, *rpcError) {
	meta, _ := params["_meta"].(map[string]any)
	if meta == nil {
		return codeExecSecrets{}, &rpcError{Code: -32602, Message: "params._meta." + codeExecMetaKey + " is required"}
	}
	block, _ := meta[codeExecMetaKey].(map[string]any)
	if block == nil {
		return codeExecSecrets{}, &rpcError{Code: -32602, Message: "params._meta." + codeExecMetaKey + " is required"}
	}
	accessToken, _ := block["accessToken"].(string)
	if accessToken == "" {
		return codeExecSecrets{}, &rpcError{Code: -32602, Message: "params._meta." + codeExecMetaKey + ".accessToken is required"}
	}
	if !hex64Re.MatchString(accessToken) {
		return codeExecSecrets{}, &rpcError{Code: -32602, Message: "params._meta." + codeExecMetaKey + ".accessToken has invalid format"}
	}
	containerAuthToken, _ := block["containerAuthToken"].(string)
	if containerAuthToken == "" {
		return codeExecSecrets{}, &rpcError{Code: -32602, Message: "params._meta." + codeExecMetaKey + ".containerAuthToken is required"}
	}
	if !hex64Re.MatchString(containerAuthToken) {
		return codeExecSecrets{}, &rpcError{Code: -32602, Message: "params._meta." + codeExecMetaKey + ".containerAuthToken has invalid format"}
	}
	encryptionKey, _ := block["encryptionKey"].(string)
	if encryptionKey == "" {
		return codeExecSecrets{}, &rpcError{Code: -32602, Message: "params._meta." + codeExecMetaKey + ".encryptionKey is required"}
	}
	if !base64Url32Re.MatchString(encryptionKey) {
		return codeExecSecrets{}, &rpcError{Code: -32602, Message: "params._meta." + codeExecMetaKey + ".encryptionKey has invalid format"}
	}
	return codeExecSecrets{
		accessToken:        accessToken,
		containerAuthToken: containerAuthToken,
		encryptionKey:      encryptionKey,
	}, nil
}

// extractUploads pulls _meta.tinfoil_code_exec.uploads, validating each
// entry's wire format. The presence of the uploads key (not its length)
// is what triggers a /sync-uploads call:
//   - field absent  → returns (nil, nil), caller skips SyncUploads
//   - field present → returns (non-nil slice, nil), caller calls SyncUploads
//   - malformed     → returns (nil, *rpcError) describing the problem
func extractUploads(params map[string]any) ([]uploadFile, *rpcError) {
	meta, _ := params["_meta"].(map[string]any)
	if meta == nil {
		return nil, nil
	}
	block, _ := meta[codeExecMetaKey].(map[string]any)
	if block == nil {
		return nil, nil
	}
	raw, ok := block["uploads"]
	if !ok {
		return nil, nil
	}
	rawList, ok := raw.([]any)
	if !ok {
		return nil, &rpcError{Code: -32602, Message: "params._meta." + codeExecMetaKey + ".uploads must be an array"}
	}
	files := make([]uploadFile, 0, len(rawList))
	for i, item := range rawList {
		entry, ok := item.(map[string]any)
		if !ok {
			return nil, &rpcError{Code: -32602, Message: fmt.Sprintf("params._meta.%s.uploads[%d] must be an object", codeExecMetaKey, i)}
		}
		fileAccessToken, _ := entry["fileAccessToken"].(string)
		filename, _ := entry["filename"].(string)
		sha, _ := entry["sha256"].(string)
		if fileAccessToken == "" {
			return nil, &rpcError{Code: -32602, Message: fmt.Sprintf("params._meta.%s.uploads[%d].fileAccessToken is required", codeExecMetaKey, i)}
		}
		if !hex64Re.MatchString(fileAccessToken) {
			return nil, &rpcError{Code: -32602, Message: fmt.Sprintf("params._meta.%s.uploads[%d].fileAccessToken has invalid format", codeExecMetaKey, i)}
		}
		if filename == "" {
			return nil, &rpcError{Code: -32602, Message: fmt.Sprintf("params._meta.%s.uploads[%d].filename is required", codeExecMetaKey, i)}
		}
		if sha == "" || !hex64Re.MatchString(sha) {
			return nil, &rpcError{Code: -32602, Message: fmt.Sprintf("params._meta.%s.uploads[%d].sha256 must be 64 hex chars", codeExecMetaKey, i)}
		}
		files = append(files, uploadFile{FileAccessToken: fileAccessToken, Filename: filename, Sha256: sha})
	}
	return files, nil
}
