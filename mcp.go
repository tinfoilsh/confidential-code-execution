package main

import (
	"net/http"
)

type jsonRPCRequest struct {
	JSONRPC string         `json:"jsonrpc"`
	ID      any            `json:"id,omitempty"`
	Method  string         `json:"method"`
	Params  map[string]any `json:"params,omitempty"`
}

type jsonRPCResponse struct {
	JSONRPC string `json:"jsonrpc"`
	ID      any    `json:"id,omitempty"`
	Result  any    `json:"result,omitempty"`
	Error   *rpcError `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// HandleMCPRequest processes a JSON-RPC 2.0 MCP request.
// Returns (status, response). If response is nil, the request was a notification.
func HandleMCPRequest(m *Manager, headers http.Header, req jsonRPCRequest) (int, *jsonRPCResponse) {
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
		sessionID := headers.Get("X-Session-Id")
		if sessionID == "" {
			resp.Error = &rpcError{Code: -32602, Message: "X-Session-Id header is required"}
			return http.StatusBadRequest, resp
		}
		name, _ := req.Params["name"].(string)
		args, _ := req.Params["arguments"].(map[string]any)
		handler, ok := ToolHandlers[name]
		if !ok {
			resp.Error = &rpcError{Code: -32601, Message: "unknown tool: " + name}
			return http.StatusBadRequest, resp
		}
		text, err := handler(m, sessionID, args)
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
