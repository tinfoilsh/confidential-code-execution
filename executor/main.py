import json
import subprocess
from http.server import BaseHTTPRequestHandler, HTTPServer


class ExecHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != "/exec":
            self.send_error(404)
            return

        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length))
        command = body.get("command", "")

        if not command:
            self.send_response(400)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"error": "command is required"}).encode())
            return

        try:
            result = subprocess.run(
                ["bash", "-c", command],
                capture_output=True,
                text=True,
                timeout=30,
            )
            response = {
                "stdout": result.stdout,
                "stderr": result.stderr,
                "exit_code": result.returncode,
            }
        except subprocess.TimeoutExpired:
            response = {
                "stdout": "",
                "stderr": "command timed out (30s)",
                "exit_code": -1,
            }

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(response).encode())

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
