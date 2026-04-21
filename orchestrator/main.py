import dataclasses
import json
import os
import ssl
import threading
import time
import urllib.error
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ADMIN_API_KEY = os.environ["ADMIN_API_KEY"]
POOL_SIZE = int(os.environ.get("POOL_SIZE", "3"))
PORT = int(os.environ.get("PORT", "7000"))
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "2"))
CONFIG_REPO = os.environ.get("CONFIG_REPO", "tinfoilsh/confidential-code-execution")
CONFIG_TAG = os.environ.get("CONFIG_TAG", "v0.0.2")
DEBUG_MODE = os.environ.get("DEBUG_MODE", "true").lower() == "true"

API_BASE = "https://api.tinfoil.sh"


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------
@dataclasses.dataclass
class ContainerRecord:
    id: str  # Tinfoil container UUID from POST response
    name: str  # "exec-a1b2c3d4"
    domain: str  # from POST response "domain" field
    status: str  # deploying | ready | assigned | deleting
    created_at: float


# ---------------------------------------------------------------------------
# Shared state (thread-safe)
# ---------------------------------------------------------------------------
_lock = threading.Lock()
_condition = threading.Condition(_lock)
_warm_pool: list[ContainerRecord] = []
_inflight: list[ContainerRecord] = []
_sessions: dict[str, ContainerRecord] = {}

# ---------------------------------------------------------------------------
# Tinfoil controlplane API helpers
# ---------------------------------------------------------------------------
# Trust default CAs for api.tinfoil.sh
_ssl_ctx = ssl.create_default_context()


def _api_request(
    method: str, path: str, body: dict | None = None
) -> tuple[int, dict | None]:
    url = f"{API_BASE}{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {ADMIN_API_KEY}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30, context=_ssl_ctx) as resp:
            if resp.status == 204:
                return 204, None
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        resp_body = None
        try:
            resp_body = json.loads(e.read())
        except Exception:
            pass
        print(f"orchestrator: API error {e.code} {method} {path}: {resp_body}")
        return e.code, resp_body


def _create_container() -> ContainerRecord | None:
    name = f"exec-{uuid.uuid4().hex[:8]}"
    status_code, data = _api_request(
        "POST",
        "/api/containers",
        {
            "name": name,
            "repo": CONFIG_REPO,
            "tag": CONFIG_TAG,
            "debug": DEBUG_MODE,
        },
    )
    if status_code != 201 or data is None:
        print(f"orchestrator: failed to create container {name}: {status_code}")
        return None
    return ContainerRecord(
        id=data["id"],
        name=name,
        domain=data["domain"],
        status="deploying",
        created_at=time.time(),
    )


def _poll_container(container_id: str) -> str | None:
    """Returns the current status string, or None on error."""
    status_code, data = _api_request("GET", f"/api/containers/{container_id}")
    if status_code != 200 or data is None:
        return None
    return data.get("status")


def _delete_container(container_id: str) -> None:
    _api_request("DELETE", f"/api/containers/{container_id}")


# ---------------------------------------------------------------------------
# Pool management
# ---------------------------------------------------------------------------
def _replenish_pool() -> list[ContainerRecord]:
    """Create containers to fill the pool. Returns newly created records."""
    with _lock:
        needed = POOL_SIZE - len(_warm_pool) - len(_inflight)
    new_records = []
    for _ in range(needed):
        rec = _create_container()
        if rec:
            new_records.append(rec)
            print(f"orchestrator: created container {rec.name} ({rec.id})")
    if new_records:
        with _lock:
            _inflight.extend(new_records)
    return new_records


def _poll_inflight() -> None:
    """Poll all inflight containers and move ready/failed ones."""
    with _lock:
        to_poll = list(_inflight)

    ready = []
    failed = []
    for rec in to_poll:
        status = _poll_container(rec.id)
        if status == "ready":
            rec.status = "ready"
            ready.append(rec)
            print(f"orchestrator: container {rec.name} is ready")
        elif status == "failed":
            failed.append(rec)
            print(f"orchestrator: container {rec.name} failed")

    if ready or failed:
        with _condition:
            for rec in ready:
                if rec in _inflight:
                    _inflight.remove(rec)
                    _warm_pool.append(rec)
            for rec in failed:
                if rec in _inflight:
                    _inflight.remove(rec)
            if ready:
                _condition.notify_all()


def _pool_manager_loop() -> None:
    """Background daemon loop that keeps the warm pool full."""
    while True:
        try:
            _replenish_pool()
            _poll_inflight()
        except Exception as e:
            print(f"orchestrator: pool manager error: {e}")
        time.sleep(POLL_INTERVAL)


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------
def _get_or_assign(session_id: str) -> ContainerRecord | None:
    """Assign a container to a session, blocking up to 60s if pool is empty."""
    with _condition:
        if session_id in _sessions:
            return _sessions[session_id]

        deadline = time.time() + 60
        while not _warm_pool:
            remaining = deadline - time.time()
            if remaining <= 0:
                return None
            _condition.wait(timeout=remaining)

        rec = _warm_pool.pop(0)
        rec.status = "assigned"
        _sessions[session_id] = rec
        print(f"orchestrator: assigned {rec.name} to session {session_id}")
        return rec


def _cleanup_session(session_id: str) -> ContainerRecord | None:
    """Release a session and schedule its container for deletion."""
    with _lock:
        rec = _sessions.pop(session_id, None)
    if rec:
        rec.status = "deleting"
        print(f"orchestrator: cleaning up {rec.name} for session {session_id}")
        threading.Thread(target=_delete_container, args=(rec.id,), daemon=True).start()
    return rec


# ---------------------------------------------------------------------------
# Proxying
# ---------------------------------------------------------------------------
def _proxy(domain: str, path: str, body: bytes) -> tuple[int, bytes]:
    """Proxy a request to a container's api-server over HTTPS."""
    url = f"https://{domain}{path}"
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=35, context=_ssl_ctx) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()
    except urllib.error.URLError as e:
        return 502, json.dumps({"error": f"container unavailable: {e}"}).encode()


# ---------------------------------------------------------------------------
# HTTP handler
# ---------------------------------------------------------------------------
class OrchestratorHandler(BaseHTTPRequestHandler):
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

    def do_POST(self):
        if self.path in ("/exec", "/read", "/write"):
            self._handle_proxy(self.path)
        elif self.path == "/cleanup":
            self._handle_cleanup()
        else:
            self.send_error(404)

    def do_GET(self):
        if self.path == "/health":
            with _lock:
                self._respond(
                    200,
                    {
                        "status": "ok",
                        "warm_pool": len(_warm_pool),
                        "inflight": len(_inflight),
                        "sessions": len(_sessions),
                        "pool_target": POOL_SIZE,
                    },
                )
            return
        self.send_error(404)

    def _handle_proxy(self, path: str):
        body = self._read_body()
        session_id = body.get("sessionId")
        if not session_id:
            self._respond(400, {"error": "sessionId is required"})
            return

        rec = _get_or_assign(session_id)
        if rec is None:
            self._respond(
                503, {"error": "no containers available (timed out after 60s)"}
            )
            return

        # Strip sessionId before forwarding — executor doesn't know about sessions
        forward_body = {k: v for k, v in body.items() if k != "sessionId"}
        status, result = _proxy(rec.domain, path, json.dumps(forward_body).encode())
        self._respond(status, result)

    def _handle_cleanup(self):
        body = self._read_body()
        session_id = body.get("sessionId")
        if not session_id:
            self._respond(400, {"error": "sessionId is required"})
            return
        rec = _cleanup_session(session_id)
        if rec is None:
            self._respond(404, {"error": f"no session found for {session_id}"})
            return
        self._respond(200, {"status": "cleaned up", "container": rec.name})

    def log_message(self, format, *args):
        print(f"orchestrator: {args[0]}")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print(
        f"orchestrator: pool_size={POOL_SIZE} poll_interval={POLL_INTERVAL}s debug={DEBUG_MODE}"
    )
    print(f"orchestrator: repo={CONFIG_REPO} tag={CONFIG_TAG}")

    # Start pool manager background thread
    pool_thread = threading.Thread(target=_pool_manager_loop, daemon=True)
    pool_thread.start()

    server = ThreadingHTTPServer(("0.0.0.0", PORT), OrchestratorHandler)
    print(f"orchestrator listening on :{PORT}")
    server.serve_forever()
