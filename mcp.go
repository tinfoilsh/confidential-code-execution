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
//
// ctx is the per-request context — pass r.Context() from the HTTP handler.
// Per-session attrs (pubkey, resume DEK) are stamped onto a child context
// via WithSessionAttrs and read back at the GetOrAssign call site, so
// they live exactly as long as the request goroutine.
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
		sessionID := headers.Get("X-Session-Id")
		if sessionID == "" {
			resp.Error = &rpcError{Code: -32602, Message: "X-Session-Id header is required"}
			return http.StatusBadRequest, resp
		}
		// Code execution is webapp-only for v1. Verify the bearer is a
		// Clerk JWT (via controlplane whoami), bind the session to that
		// user on the first call, and reject any later request whose
		// verified identity doesn't match the binding.
		bearer := extractBearer(headers.Get("Authorization"))
		if _, err := m.AuthorizeSession(ctx, sessionID, bearer); err != nil {
			switch {
			case errors.Is(err, ErrAuthRequired):
				resp.Error = &rpcError{Code: -32001, Message: err.Error()}
				return http.StatusUnauthorized, resp
			case errors.Is(err, ErrIdentityMismatch):
				resp.Error = &rpcError{Code: -32002, Message: err.Error()}
				return http.StatusForbidden, resp
			default:
				resp.Error = &rpcError{Code: -32603, Message: "auth check failed: " + err.Error()}
				return http.StatusInternalServerError, resp
			}
		}
		// Stash per-request attrs on ctx. X-Exec-Pubkey is what
		// GetOrAssign stamps onto c.Pubkey so eviction-time snapshotting
		// can wrap the DEK to it. X-Exec-Resume-Dek, when present, tells
		// GetOrAssign to fetch + decrypt + push the snapshot tar into the
		// fresh container's /restore before the first tool call exposes it.
		ctx = WithSessionAttrs(ctx,
			headers.Get("X-Exec-Pubkey"),
			headers.Get("X-Exec-Resume-Dek"))
		name, _ := req.Params["name"].(string)
		args, _ := req.Params["arguments"].(map[string]any)
		handler, ok := ToolHandlers[name]
		if !ok {
			resp.Error = &rpcError{Code: -32601, Message: "unknown tool: " + name}
			return http.StatusBadRequest, resp
		}
		text, err := handler(ctx, m, sessionID, args)
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
