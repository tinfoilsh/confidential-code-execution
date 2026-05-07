package main

import (
	"context"
	"errors"
	"net/http"
)

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
//
// ctx is the per-request context — pass r.Context() from the HTTP handler.
// The user's symmetric Code Execution Encryption Key is stamped onto a
// child context via WithCodeExecutionEncryptionKey and read back at the
// GetOrAssign call site, so it lives exactly as long as the request
// goroutine.
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
		accessToken := headers.Get("X-Code-Execution-Access-Token")
		if accessToken == "" {
			resp.Error = &rpcError{Code: -32602, Message: "X-Code-Execution-Access-Token header is required"}
			return http.StatusBadRequest, resp
		}
		// Gate the request on a recognized api_key via controlplane
		// /api/shim/identity. Yes/no only — buckets re-resolves the same
		// bearer itself when it needs the (user_id, org_id) prefix.
		bearer := extractBearer(headers.Get("Authorization"))
		if err := m.AuthorizeSession(ctx, bearer); err != nil {
			if errors.Is(err, ErrAuthRequired) {
				resp.Error = &rpcError{Code: -32001, Message: err.Error()}
				return http.StatusUnauthorized, resp
			}
			resp.Error = &rpcError{Code: -32603, Message: "auth check failed: " + err.Error()}
			return http.StatusInternalServerError, resp
		}
		// Stash the Code Execution Encryption Key on ctx. GetOrAssign reads
		// it back to (a) cache it on c.CodeExecutionEncryptionKey for
		// eviction-time encryption via buckets, and (b) drive the
		// restore-on-assign fetch from buckets before the fresh container
		// is exposed to traffic.
		ctx = WithCodeExecutionEncryptionKey(ctx, headers.Get("X-Code-Execution-Encryption-Key"))
		// Stash the bearer too so GetOrAssign can cache it on the
		// container for buckets calls (restore-on-assign and
		// eviction-time snapshot).
		ctx = WithBearer(ctx, bearer)
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
