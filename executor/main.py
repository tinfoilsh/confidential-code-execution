import base64
import json
import os
import subprocess
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import PurePosixPath

WORKSPACE = "/workspace"


def resolve_path(path: str) -> str:
    p = PurePosixPath(path)
    if not p.is_absolute():
        p = PurePosixPath(WORKSPACE) / p
    return str(p)


class ExecHandler(BaseHTTPRequestHandler):
    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        return json.loads(self.rfile.read(length))

    def _respond(self, status, data):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def do_POST(self):
        if self.path == "/exec":
            self._handle_exec()
        elif self.path == "/read":
            self._handle_read()
        elif self.path == "/write":
            self._handle_write()
        else:
            self.send_error(404)

    def _handle_exec(self):
        body = self._read_body()
        command = body.get("command", "")

        if not command:
            self._respond(400, {"error": "command is required"})
            return

        try:
            result = subprocess.run(
                ["bash", "-c", command],
                capture_output=True,
                text=True,
                timeout=30,
                cwd=WORKSPACE,
            )
            self._respond(
                200,
                {
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                    "exit_code": result.returncode,
                },
            )
        except subprocess.TimeoutExpired:
            self._respond(
                200,
                {
                    "stdout": "",
                    "stderr": "command timed out (30s)",
                    "exit_code": -1,
                },
            )

    def _handle_read(self):
        body = self._read_body()
        path = body.get("path", "")

        if not path:
            self._respond(400, {"error": "path is required"})
            return

        resolved = resolve_path(path)

        if not os.path.exists(resolved):
            self._respond(404, {"error": f"file not found: {path}"})
            return

        if os.path.isdir(resolved):
            self._respond(400, {"error": f"path is a directory: {path}"})
            return

        with open(resolved, "rb") as f:
            contents = base64.b64encode(f.read()).decode("ascii")

        self._respond(200, {"path": path, "contents": contents})

    def _handle_write(self):
        body = self._read_body()
        path = body.get("path", "")
        contents = body.get("contents", "")

        if not path:
            self._respond(400, {"error": "path is required"})
            return

        resolved = resolve_path(path)

        # create parent directories
        parent = os.path.dirname(resolved)
        os.makedirs(parent, exist_ok=True)

        data = base64.b64decode(contents)
        with open(resolved, "wb") as f:
            f.write(data)

        self._respond(200, {"path": path, "size": len(data)})

    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "ok"}).encode())
            return
        self.send_error(404)

    def log_message(self, format, *args):
        print(f"executor: {args[0]}")


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", 9000), ExecHandler)
    print("executor listening on :9000")
    server.serve_forever()
