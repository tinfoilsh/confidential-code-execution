"""
MCP Streamable HTTP server for code execution tools.

Exposes bash, view, str_replace, create, and insert tools via the
Model Context Protocol, proxying execution to the orchestrator HTTP API.

The server is stateless from the MCP perspective (each POST is independent)
but maps the router's X-Tinfoil-Tool-Request-Id header to an orchestrator
session ID so all tool calls from one router request share the same container.

Usage:
    python mcp.py
    MCP_PORT=8093 python mcp.py
    ORCHESTRATOR_URL=http://other-host:7070 python mcp.py

Surfaces:
    POST /mcp     - MCP Streamable HTTP endpoint (JSON-RPC)
    GET  /health  - health check
"""

import base64
import json
import os
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ORCHESTRATOR_URL = os.environ.get("ORCHESTRATOR_URL", "http://localhost:7070")
PORT = int(os.environ.get("MCP_PORT", "8092"))

PROTOCOL_VERSION = "2025-03-26"
SERVER_NAME = "confidential-code-execution"
SERVER_VERSION = "0.1.0"

# ---------------------------------------------------------------------------
# MCP tool definitions
# ---------------------------------------------------------------------------
TOOLS = [
    {
        "name": "bash",
        "description": (
            "Execute a bash command in a sandboxed container. "
            "Returns stdout, stderr, and exit code."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "command": {
                    "type": "string",
                    "description": "The bash command to execute",
                },
            },
            "required": ["command"],
        },
    },
    {
        "name": "view",
        "description": (
            "View a file's contents with line numbers. "
            "Returns the full file or a specific line range."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Absolute or workspace-relative path to the file",
                },
                "view_range": {
                    "type": "array",
                    "items": {"type": "integer"},
                    "minItems": 2,
                    "maxItems": 2,
                    "description": (
                        "Optional [start_line, end_line] (1-indexed, inclusive). "
                        "Omit to view the entire file."
                    ),
                },
            },
            "required": ["path"],
        },
    },
    {
        "name": "str_replace",
        "description": (
            "Replace an exact string occurrence in a file. "
            "The old_str must appear exactly once in the file."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Absolute or workspace-relative path to the file",
                },
                "old_str": {
                    "type": "string",
                    "description": "The exact string to find (must be unique in the file)",
                },
                "new_str": {
                    "type": "string",
                    "description": "The replacement string",
                },
            },
            "required": ["path", "old_str", "new_str"],
        },
    },
    {
        "name": "create",
        "description": "Create a new file with the given contents. Fails if the file already exists.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Absolute or workspace-relative path for the new file",
                },
                "file_text": {
                    "type": "string",
                    "description": "The full contents of the file to create",
                },
            },
            "required": ["path", "file_text"],
        },
    },
    {
        "name": "insert",
        "description": (
            "Insert text after a specific line number in a file. "
            "Use line 0 to insert at the beginning."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Absolute or workspace-relative path to the file",
                },
                "line_number": {
                    "type": "integer",
                    "description": (
                        "Insert after this line (0 = beginning of file, "
                        "1 = after first line)"
                    ),
                },
                "text": {
                    "type": "string",
                    "description": "The text to insert",
                },
            },
            "required": ["path", "line_number", "text"],
        },
    },
]


# ---------------------------------------------------------------------------
# Orchestrator proxy helpers
# ---------------------------------------------------------------------------
def _orchestrator_request(path: str, body: dict, timeout: int = 35) -> dict:
    """POST JSON to the orchestrator and return the parsed response."""
    req = urllib.request.Request(
        f"{ORCHESTRATOR_URL}{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def _read_file(session_id: str, path: str) -> str:
    result = _orchestrator_request("/read", {"sessionId": session_id, "path": path})
    if "error" in result:
        raise RuntimeError(result["error"])
    return base64.b64decode(result["contents"]).decode("utf-8")


def _write_file(session_id: str, path: str, content: str) -> dict:
    encoded = base64.b64encode(content.encode("utf-8")).decode("ascii")
    return _orchestrator_request(
        "/write", {"sessionId": session_id, "path": path, "contents": encoded}
    )


def _file_exists(session_id: str, path: str) -> bool:
    try:
        _orchestrator_request("/read", {"sessionId": session_id, "path": path})
        return True
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
        raise


# ---------------------------------------------------------------------------
# Tool handlers
# ---------------------------------------------------------------------------
def handle_bash(session_id: str, args: dict) -> str:
    result = _orchestrator_request(
        "/exec", {"sessionId": session_id, "command": args["command"]}
    )
    parts = []
    if result.get("stdout"):
        parts.append(f"stdout:\n{result['stdout']}")
    if result.get("stderr"):
        parts.append(f"stderr:\n{result['stderr']}")
    parts.append(f"exit_code: {result.get('exit_code', -1)}")
    return "\n".join(parts)


def handle_view(session_id: str, args: dict) -> str:
    content = _read_file(session_id, args["path"])
    lines = content.splitlines()
    view_range = args.get("view_range")

    if view_range:
        start, end = view_range
        start = max(1, start)
        end = min(len(lines), end)
        selected = lines[start - 1 : end]
        numbered = [f"{i:6d}\t{line}" for i, line in enumerate(selected, start=start)]
    else:
        numbered = [f"{i:6d}\t{line}" for i, line in enumerate(lines, start=1)]

    return "\n".join(numbered)


def handle_str_replace(session_id: str, args: dict) -> str:
    path = args["path"]
    old_str = args["old_str"]
    new_str = args["new_str"]

    content = _read_file(session_id, path)
    count = content.count(old_str)
    if count == 0:
        return f"error: old_str not found in {path}"
    if count > 1:
        return f"error: old_str appears {count} times in {path} (must be unique)"

    new_content = content.replace(old_str, new_str, 1)
    _write_file(session_id, path, new_content)

    new_lines = new_content.splitlines()
    replacement_first_line = new_str.splitlines()[0] if new_str else ""
    for i, line in enumerate(new_lines):
        if replacement_first_line and replacement_first_line in line:
            start = max(0, i - 2)
            end = min(len(new_lines), i + len(new_str.splitlines()) + 2)
            snippet = [f"{j+1:6d}\t{new_lines[j]}" for j in range(start, end)]
            return f"Replaced in {path}:\n" + "\n".join(snippet)

    return f"Replaced in {path}"


def handle_create(session_id: str, args: dict) -> str:
    path = args["path"]
    if _file_exists(session_id, path):
        return f"error: file already exists: {path}"
    result = _write_file(session_id, path, args["file_text"])
    return f"Created {path} ({result.get('size', 0)} bytes)"


def handle_insert(session_id: str, args: dict) -> str:
    path = args["path"]
    line_number = args["line_number"]
    text = args["text"]

    content = _read_file(session_id, path)
    lines = content.splitlines(keepends=True)

    insert_lines = text.splitlines(keepends=True)
    if insert_lines and not insert_lines[-1].endswith("\n"):
        insert_lines[-1] += "\n"

    idx = max(0, min(line_number, len(lines)))
    new_lines = lines[:idx] + insert_lines + lines[idx:]
    new_content = "".join(new_lines)
    _write_file(session_id, path, new_content)

    start = max(0, idx - 1)
    end = min(len(new_lines), idx + len(insert_lines) + 1)
    display = new_content.splitlines()
    snippet = [f"{j+1:6d}\t{display[j]}" for j in range(start, end)]
    return f"Inserted at line {line_number} in {path}:\n" + "\n".join(snippet)


TOOL_HANDLERS = {
    "bash": handle_bash,
    "view": handle_view,
    "str_replace": handle_str_replace,
    "create": handle_create,
    "insert": handle_insert,
}


# ---------------------------------------------------------------------------
# JSON-RPC / MCP handler
# ---------------------------------------------------------------------------
class MCPHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != "/mcp":
            self.send_error(404)
            return

        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length)
        body = json.loads(raw)

        # JSON-RPC notification (no "id") — acknowledge and move on.
        if "id" not in body:
            self.send_response(202)
            self.end_headers()
            return

        request_id = body["id"]
        method = body.get("method", "")

        if method == "initialize":
            self._respond_result(
                request_id,
                {
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": {"tools": {}},
                    "serverInfo": {
                        "name": SERVER_NAME,
                        "version": SERVER_VERSION,
                    },
                },
            )

        elif method == "tools/list":
            self._respond_result(request_id, {"tools": TOOLS})

        elif method == "tools/call":
            params = body.get("params", {})
            result = self._handle_tool_call(params)
            self._respond_result(request_id, result)

        else:
            self._respond_error(request_id, -32601, f"Method not found: {method}")

    def _handle_tool_call(self, params: dict) -> dict:
        name = params.get("name", "")
        arguments = params.get("arguments", {})

        # Use the router's request ID as the orchestrator session ID so
        # all tool calls from one router request share the same container.
        session_id = self.headers.get("X-Tinfoil-Tool-Request-Id", "mcp-default")

        handler = TOOL_HANDLERS.get(name)
        if not handler:
            return {
                "isError": True,
                "content": [{"type": "text", "text": f"Unknown tool: {name}"}],
            }

        try:
            output = handler(session_id, arguments)
            return {"content": [{"type": "text", "text": output}]}
        except Exception as e:
            return {
                "isError": True,
                "content": [{"type": "text", "text": f"error: {e}"}],
            }

    def do_GET(self):
        if self.path == "/health":
            self._respond_json(200, {"status": "ok"})
        else:
            self.send_error(404)

    # --- response helpers ---

    def _respond_result(self, request_id, result):
        self._respond_json(
            200,
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": result,
            },
        )

    def _respond_error(self, request_id, code, message):
        self._respond_json(
            200,
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": code, "message": message},
            },
        )

    def _respond_json(self, status, data):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        print(f"mcp: {args[0]}")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print(f"mcp: orchestrator={ORCHESTRATOR_URL}")
    print(f"mcp: tools={[t['name'] for t in TOOLS]}")
    server = ThreadingHTTPServer(("0.0.0.0", PORT), MCPHandler)
    print(f"mcp: listening on :{PORT}")
    server.serve_forever()
