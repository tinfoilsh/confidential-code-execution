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
MAX_CONTAINERS = int(os.environ.get("MAX_CONTAINERS", "10"))
PORT = int(os.environ.get("PORT", "7070"))
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
_failed: list[ContainerRecord] = []  # keeps last N failures for display
_fail_count: int = 0  # total cumulative failures
_api_errors: int = 0  # total create-container API errors
_shutting_down: bool = False

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
            "User-Agent": "tinfoil-orchestrator/1.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30, context=_ssl_ctx) as resp:
            if resp.status == 204:
                return 204, None
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        raw = e.read()
        resp_body = None
        try:
            resp_body = json.loads(raw)
        except Exception:
            pass
        print(f"orchestrator: API error {e.code} {method} {path}: {resp_body}")
        return e.code, resp_body


def _create_container() -> ContainerRecord | None:
    name = f"daniel-exec-{uuid.uuid4().hex[:8]}"
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
        global _api_errors
        with _lock:
            _api_errors += 1
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
        total = len(_warm_pool) + len(_inflight) + len(_sessions)
        target = min(len(_sessions) + POOL_SIZE, MAX_CONTAINERS)
        needed = target - total
    new_records = []
    for _ in range(needed):
        rec = _create_container()
        if rec is None:
            break  # stop on first failure — don't spam a broken API
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
            global _fail_count
            for rec in failed:
                if rec in _inflight:
                    _inflight.remove(rec)
                    rec.status = "failed"
                    _failed.append(rec)
                    _fail_count += 1
            # keep only last 10 failures for display
            while len(_failed) > 10:
                _failed.pop(0)
            if ready:
                _condition.notify_all()


def _pool_manager_loop() -> None:
    """Background daemon loop that keeps the warm pool full."""
    consecutive_failures = 0
    while not _shutting_down:
        created: list[ContainerRecord] = []
        try:
            if not _shutting_down:
                created = _replenish_pool()
            _poll_inflight()
            if created:
                consecutive_failures = 0
            elif consecutive_failures > 0:
                # still failing — no need to check, just wait
                pass
        except Exception as e:
            print(f"orchestrator: pool manager error: {e}")

        # Check if last replenish created nothing when it should have
        with _lock:
            needed = (
                min(len(_sessions) + POOL_SIZE, MAX_CONTAINERS)
                - len(_warm_pool)
                - len(_inflight)
                - len(_sessions)
            )
        if needed > 0 and not created:
            consecutive_failures += 1
        else:
            consecutive_failures = 0

        # Backoff: 2s, 4s, 8s, 16s, max 30s
        delay = min(POLL_INTERVAL * (2 ** min(consecutive_failures, 4)), 30)
        if consecutive_failures > 0 and consecutive_failures % 5 == 1:
            print(
                f"orchestrator: create failing, backoff {delay}s (consecutive failures: {consecutive_failures})"
            )
        time.sleep(delay)


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------
def _get_or_assign(session_id: str) -> tuple[ContainerRecord | None, str | None]:
    """Assign a container to a session, blocking up to 60s if pool is empty.
    Returns (record, error_message). error_message is set when at capacity."""
    with _condition:
        if session_id in _sessions:
            return _sessions[session_id], None

        if len(_sessions) >= MAX_CONTAINERS:
            return None, f"at capacity ({MAX_CONTAINERS} sessions)"

        deadline = time.time() + 60
        while not _warm_pool:
            remaining = deadline - time.time()
            if remaining <= 0:
                return None, "no containers available (timed out after 60s)"
            _condition.wait(
                timeout=remaining
            )  # Only allows one thread to try & exit the while loop at a time

        rec = _warm_pool.pop(0)
        rec.status = "assigned"
        _sessions[session_id] = rec
        print(f"orchestrator: assigned {rec.name} to session {session_id}")
        return rec, None


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
        elif self.path == "/delete-all":
            self._handle_delete_all()
        elif self.path == "/finish":
            self._handle_finish()
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
                        "max_containers": MAX_CONTAINERS,
                    },
                )
            return
        if self.path == "/metrics":
            now = time.time()
            with _lock:

                def _rec(r, session_id=None):
                    d = {
                        "id": r.id,
                        "name": r.name,
                        "status": r.status,
                        "uptime": round(now - r.created_at),
                    }
                    if session_id:
                        d["session_id"] = session_id
                    return d

                data = {
                    "warm_pool": [_rec(r) for r in _warm_pool],
                    "inflight": [_rec(r) for r in _inflight],
                    "sessions": [_rec(r, sid) for sid, r in _sessions.items()],
                    "failed": [_rec(r) for r in _failed],
                    "fail_count": _fail_count,
                    "api_errors": _api_errors,
                    "pool_target": POOL_SIZE,
                    "max_containers": MAX_CONTAINERS,
                }
            self._respond(200, data)
            return
        self.send_error(404)

    def _handle_proxy(self, path: str):
        body = self._read_body()
        session_id = body.get("sessionId")
        if not session_id:
            self._respond(400, {"error": "sessionId is required"})
            return

        rec, err = _get_or_assign(session_id)
        if rec is None:
            self._respond(503, {"error": err})
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

    def _handle_delete_all(self):
        """Delete every tracked container and clear all state.

        Safety: verifies each container exists on the controlplane and its
        name matches our local record before issuing the DELETE.
        """
        with _lock:
            all_recs = list(_warm_pool) + list(_inflight) + list(_sessions.values())
            _warm_pool.clear()
            _inflight.clear()
            _sessions.clear()
            _failed.clear()
        deleted = []
        skipped = []
        for rec in all_recs:
            # Double-check: fetch from controlplane and verify name matches
            status_code, remote = _api_request("GET", f"/api/containers/{rec.id}")
            if status_code != 200 or remote is None:
                print(
                    f"orchestrator: skip delete {rec.name} ({rec.id}) — not found on controlplane ({status_code})"
                )
                skipped.append(
                    {
                        "name": rec.name,
                        "id": rec.id,
                        "reason": "not found on controlplane",
                    }
                )
                continue
            remote_name = remote.get("name", "")
            if remote_name != rec.name:
                print(
                    f"orchestrator: skip delete {rec.name} ({rec.id}) — name mismatch: remote={remote_name}"
                )
                skipped.append(
                    {
                        "name": rec.name,
                        "id": rec.id,
                        "reason": f"name mismatch: expected {rec.name}, got {remote_name}",
                    }
                )
                continue
            print(f"orchestrator: deleting {rec.name} ({rec.id}) — verified")
            _delete_container(rec.id)
            deleted.append(rec.name)
        self._respond(
            200, {"deleted": deleted, "skipped": skipped, "count": len(deleted)}
        )

    def _handle_finish(self):
        """Stop replenishing, delete all containers, shut down the server."""
        global _shutting_down
        _shutting_down = True
        print("orchestrator: finishing — deleting all containers and shutting down")
        # Reuse delete-all logic inline
        with _lock:
            all_recs = list(_warm_pool) + list(_inflight) + list(_sessions.values())
            _warm_pool.clear()
            _inflight.clear()
            _sessions.clear()
            _failed.clear()
        deleted = []
        for rec in all_recs:
            status_code, remote = _api_request("GET", f"/api/containers/{rec.id}")
            if status_code != 200 or remote is None:
                continue
            if remote.get("name", "") != rec.name:
                continue
            print(f"orchestrator: deleting {rec.name} ({rec.id}) — verified")
            _delete_container(rec.id)
            deleted.append(rec.name)
        self._respond(200, {"status": "finished", "deleted": deleted, "count": len(deleted)})
        # Shut down the server in a background thread so the response sends first
        threading.Thread(target=self.server.shutdown, daemon=True).start()

    def log_message(self, format, *args):
        print(f"orchestrator: {args[0]}")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print(
        f"orchestrator: pool_size={POOL_SIZE} max_containers={MAX_CONTAINERS} poll_interval={POLL_INTERVAL}s debug={DEBUG_MODE}"
    )
    print(f"orchestrator: repo={CONFIG_REPO} tag={CONFIG_TAG}")

    # Start pool manager background thread
    pool_thread = threading.Thread(target=_pool_manager_loop, daemon=True)
    pool_thread.start()

    server = ThreadingHTTPServer(("0.0.0.0", PORT), OrchestratorHandler)
    print(f"orchestrator listening on :{PORT}")
    server.serve_forever()
