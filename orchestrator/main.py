"""
Orchestrator entry point.

Thin HTTP server that routes:
  POST /mcp        → MCP handler (primary tool interface)
  GET  /health     → health check
  GET  /metrics    → detailed metrics for viz.py
  POST /cleanup    → release a single session
  POST /delete-all → delete all containers
  POST /finish     → delete all + shutdown
"""

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from container_manager import ContainerManager
from mcp import handle_mcp_request

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ADMIN_API_KEY = os.environ["ADMIN_API_KEY"]
POOL_SIZE = int(os.environ.get("POOL_SIZE", "3"))
MAX_CONTAINERS = int(os.environ.get("MAX_CONTAINERS", "10"))
PORT = int(os.environ.get("PORT", "7070"))
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "2"))
CONFIG_REPO = os.environ.get("CONFIG_REPO", "tinfoilsh/code-execution-environment")
CONFIG_TAG = os.environ.get("CONFIG_TAG", "v0.0.6")
DEBUG_MODE = os.environ.get("DEBUG_MODE", "true").lower() == "true"


# ---------------------------------------------------------------------------
# HTTP handler
# ---------------------------------------------------------------------------
def make_handler(manager: ContainerManager):
    """Create the main HTTP handler class wired to the ContainerManager."""

    class OrchestratorHandler(BaseHTTPRequestHandler):
        def do_POST(self):
            if self.path == "/mcp":
                self._handle_mcp()
            elif self.path == "/cleanup":
                self._handle_cleanup()
            elif self.path == "/delete-all":
                self._handle_delete_all()
            elif self.path == "/finish":
                self._handle_finish()
            else:
                self.send_error(404)

        def do_GET(self):
            if self.path == "/health":
                self._respond(200, manager.health_info())
            elif self.path == "/metrics":
                self._respond(200, manager.metrics_info())
            else:
                self.send_error(404)

        def _read_body(self) -> dict:
            length = int(self.headers.get("Content-Length", 0))
            return json.loads(self.rfile.read(length))

        def _respond(self, status: int, data):
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            if isinstance(data, bytes):
                self.wfile.write(data)
            else:
                self.wfile.write(json.dumps(data).encode())

        def _handle_mcp(self):
            body = self._read_body()
            status, response = handle_mcp_request(manager, self.headers, body)
            if response is None:
                # Notification — just send status with no body
                self.send_response(status)
                self.end_headers()
            else:
                payload = json.dumps(response).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

        def _handle_cleanup(self):
            body = self._read_body()
            session_id = body.get("sessionId")
            if not session_id:
                self._respond(400, {"error": "sessionId is required"})
                return
            rec = manager.cleanup_session(session_id)
            if rec is None:
                self._respond(404, {"error": f"no session found for {session_id}"})
                return
            self._respond(200, {"status": "cleaned up", "container": rec.name})

        def _handle_delete_all(self):
            result = manager.cleanup_all()
            self._respond(200, result)

        def _handle_finish(self):
            result = manager.finish()
            self._respond(200, result)
            threading.Thread(target=self.server.shutdown, daemon=True).start()

        def log_message(self, format, *args):
            print(f"orchestrator: {args[0]}")

    return OrchestratorHandler


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print(
        f"orchestrator: pool_size={POOL_SIZE} max_containers={MAX_CONTAINERS} poll_interval={POLL_INTERVAL}s debug={DEBUG_MODE}"
    )
    print(f"orchestrator: repo={CONFIG_REPO} tag={CONFIG_TAG}")

    manager = ContainerManager(
        admin_api_key=ADMIN_API_KEY,
        pool_size=POOL_SIZE,
        max_containers=MAX_CONTAINERS,
        poll_interval=POLL_INTERVAL,
        config_repo=CONFIG_REPO,
        config_tag=CONFIG_TAG,
        debug_mode=DEBUG_MODE,
    )
    manager.start_pool_manager()

    Handler = make_handler(manager)
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"orchestrator listening on :{PORT}")
    server.serve_forever()
