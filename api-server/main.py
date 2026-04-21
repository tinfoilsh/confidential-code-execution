import json
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer

EXECUTOR_URL = "http://localhost:9000"


class APIHandler(BaseHTTPRequestHandler):
    ALLOWED_PATHS = {"/exec", "/read", "/write"}

    def do_POST(self):
        if self.path not in self.ALLOWED_PATHS:
            self.send_error(404)
            return

        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)

        try:
            req = urllib.request.Request(
                f"{EXECUTOR_URL}{self.path}",
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=35) as resp:
                result = resp.read()
        except urllib.error.URLError as e:
            self.send_response(502)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(
                json.dumps({"error": f"executor unavailable: {e}"}).encode()
            )
            return
        except Exception as e:
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"error": str(e)}).encode())
            return

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(result)

    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "ok"}).encode())
            return
        self.send_error(404)

    def log_message(self, format, *args):
        print(f"api: {args[0]}")


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", 8000), APIHandler)
    print("api server listening on :8000")
    server.serve_forever()
