"""
MCP JSON-RPC handler for the code execution server.

Thin protocol layer — tool schemas live in tools.py, business logic in
container_manager.py. Mirrors the websearch mcp_server.go pattern.
"""

from container_manager import ContainerManager
from tools import TOOL_HANDLERS, TOOLS

PROTOCOL_VERSION = "2025-03-26"
SERVER_NAME = "confidential-code-execution"
SERVER_VERSION = "0.1.0"


def extract_session_id(headers) -> str:
    """Extract session ID from request headers. Client resonsibility to send this. Necessary"""
    sid = headers.get("X-Session-Id")
    if sid:
        return sid
    return "mcp-default"


def handle_mcp_request(
    manager: ContainerManager, headers, body: dict
) -> tuple[int, dict | None]:
    """Process a single MCP JSON-RPC request.

    Returns (http_status, response_body). response_body is None for
    notifications (202).
    """
    # JSON-RPC notification (no "id") — acknowledge and move on.
    if "id" not in body:
        return 202, None

    request_id = body["id"]
    method = body.get("method", "")

    if method == "initialize":
        return 200, {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {"tools": {}},
                "serverInfo": {
                    "name": SERVER_NAME,
                    "version": SERVER_VERSION,
                },
            },
        }

    if method == "tools/list":
        return 200, {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {"tools": TOOLS},
        }

    if method == "tools/call":
        params = body.get("params", {})
        result = _handle_tool_call(manager, headers, params)
        return 200, {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": result,
        }

    return 200, {
        "jsonrpc": "2.0",
        "id": request_id,
        "error": {"code": -32601, "message": f"Method not found: {method}"},
    }


def _handle_tool_call(manager: ContainerManager, headers, params: dict) -> dict:
    name = params.get("name", "")
    arguments = params.get("arguments", {})
    session_id = extract_session_id(headers)

    handler = TOOL_HANDLERS.get(name)
    if not handler:
        return {
            "isError": True,
            "content": [{"type": "text", "text": f"Unknown tool: {name}"}],
        }

    try:
        output = handler(manager, session_id, arguments)
        return {"content": [{"type": "text", "text": output}]}
    except Exception as e:
        return {
            "isError": True,
            "content": [{"type": "text", "text": f"error: {e}"}],
        }
