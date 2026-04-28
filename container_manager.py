import base64
import dataclasses
import json
import ssl
import threading
import time
import urllib.error
import urllib.request
import uuid

import httpx
from tinfoil.client import SecureClient

API_BASE = "https://api.tinfoil.sh"

# Trust default CAs for api.tinfoil.sh
_ssl_ctx = ssl.create_default_context()


@dataclasses.dataclass
class ContainerRecord:
    id: str  # Tinfoil container UUID from POST response
    name: str  # "exec-a1b2c3d4"
    domain: str  # from POST response "domain" field
    status: str  # deploying | ready | assigned | deleting
    created_at: float
    ssh_port: int = 0  # SSH port from create response
    assigned_at: float = 0.0  # when session was assigned


class ContainerManager:
    def __init__(
        self,
        admin_api_key: str,
        config_repo: str,
        config_tag: str,
        pool_size: int = 3,
        max_containers: int = 10,
        poll_interval: int = 2,
        debug_mode: bool = True,
        verify_attestation: bool = True,
    ):
        self.admin_api_key = admin_api_key
        self.pool_size = pool_size
        self.max_containers = max_containers
        self.poll_interval = poll_interval
        self.config_repo = config_repo
        self.config_tag = config_tag
        self.debug_mode = debug_mode
        self.verify_attestation = verify_attestation

        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._warm_pool: list[ContainerRecord] = []
        self._inflight: list[ContainerRecord] = []
        self._sessions: dict[str, ContainerRecord] = {}
        self._failed: list[ContainerRecord] = []
        self._fail_count: int = 0
        self._api_errors: int = 0
        self._shutting_down: bool = False
        self._http_clients: dict[str, httpx.Client] = {}  # container id -> attested client

    # ------------------------------------------------------------------
    # Controlplane API
    # ------------------------------------------------------------------

    def _api_request(
        self, method: str, path: str, body: dict | None = None
    ) -> tuple[int, dict | None]:
        url = f"{API_BASE}{path}"
        data = json.dumps(body).encode() if body else None
        req = urllib.request.Request(
            url,
            data=data,
            method=method,
            headers={
                "Authorization": f"Bearer {self.admin_api_key}",
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

    def _create_container(self) -> ContainerRecord | None:
        name = f"daniel-exec-{uuid.uuid4().hex[:8]}"
        status_code, data = self._api_request(
            "POST",
            "/api/containers",
            {
                "name": name,
                "repo": self.config_repo,
                "tag": self.config_tag,
                "debug": self.debug_mode,
                "ssh_keys": ["daniel"],
            },
        )
        if status_code != 201 or data is None:
            print(f"orchestrator: failed to create container {name}: {status_code}")
            with self._lock:
                self._api_errors += 1
            return None
        return ContainerRecord(
            id=data["id"],
            name=name,
            domain=data["domain"],
            status="deploying",
            created_at=time.time(),
            ssh_port=data.get("ssh_port", 0),
        )

    def _poll_container(self, container_id: str) -> str | None:
        """Returns the current status string, or None on error."""
        status_code, data = self._api_request("GET", f"/api/containers/{container_id}")
        if status_code != 200 or data is None:
            return None
        return data.get("status")

    def _delete_container(self, container_id: str) -> None:
        self._api_request("DELETE", f"/api/containers/{container_id}")

    # ------------------------------------------------------------------
    # Attestation
    # ------------------------------------------------------------------

    def _create_attested_client(self, rec: ContainerRecord) -> httpx.Client:
        """Return an httpx.Client for the container.

        If verify_attestation is True, the client pins the enclave's TLS public
        key after verifying its attestation. Otherwise, returns a plain client
        with default CA verification (no enclave attestation).
        """
        if not self.verify_attestation:
            print(f"orchestrator: skipping attestation for {rec.name} ({rec.domain})")
            return httpx.Client(follow_redirects=True)
        sc = SecureClient(rec.domain, self.config_repo)
        client = sc.make_secure_http_client()
        print(f"orchestrator: attestation verified for {rec.name} ({rec.domain})")
        return client

    def _get_http_client(self, rec: ContainerRecord) -> httpx.Client:
        """Get the attested httpx.Client for a container, or raise."""
        client = self._http_clients.get(rec.id)
        if client is None:
            raise RuntimeError(f"no attested client for container {rec.name}")
        return client

    def _close_http_client(self, container_id: str) -> None:
        """Close and remove the cached httpx.Client for a container."""
        client = self._http_clients.pop(container_id, None)
        if client:
            client.close()

    # ------------------------------------------------------------------
    # Pool management
    # ------------------------------------------------------------------

    def _replenish_pool(self) -> list[ContainerRecord]:
        """Create containers to fill the pool. Returns newly created records."""
        with self._lock:
            total = len(self._warm_pool) + len(self._inflight) + len(self._sessions)
            target = min(len(self._sessions) + self.pool_size, self.max_containers)
            needed = target - total
        new_records = []
        for _ in range(needed):
            rec = self._create_container()
            if rec is None:
                break
            new_records.append(rec)
            print(f"orchestrator: created container {rec.name} ({rec.id})")
        if new_records:
            with self._lock:
                self._inflight.extend(new_records)
        return new_records

    def _poll_inflight(self) -> None:
        """Poll all inflight containers and move ready/failed ones."""
        with self._lock:
            to_poll = list(self._inflight)

        ready = []
        failed = []
        for rec in to_poll:
            status = self._poll_container(rec.id)
            if status == "ready":
                # Verify attestation before marking as ready
                try:
                    client = self._create_attested_client(rec)
                    self._http_clients[rec.id] = client
                    rec.status = "ready"
                    ready.append(rec)
                    print(f"orchestrator: container {rec.name} is ready")
                except Exception as e:
                    print(f"orchestrator: attestation failed for {rec.name}: {e}")
                    failed.append(rec)
            elif status == "failed":
                failed.append(rec)
                print(f"orchestrator: container {rec.name} failed")

        if ready or failed:
            with self._condition:
                for rec in ready:
                    if rec in self._inflight:
                        self._inflight.remove(rec)
                        self._warm_pool.append(rec)
                for rec in failed:
                    if rec in self._inflight:
                        self._inflight.remove(rec)
                        rec.status = "failed"
                        self._failed.append(rec)
                        self._fail_count += 1
                # keep only last 10 failures for display
                while len(self._failed) > 10:
                    self._failed.pop(0)
                if ready:
                    self._condition.notify_all()

    def start_pool_manager(self) -> threading.Thread:
        """Start the background pool manager daemon thread."""
        t = threading.Thread(target=self._pool_manager_loop, daemon=True)
        t.start()
        return t

    def _pool_manager_loop(self) -> None:
        """Background daemon loop that keeps the warm pool full."""
        consecutive_failures = 0
        while not self._shutting_down:
            created: list[ContainerRecord] = []
            try:
                if not self._shutting_down:
                    created = self._replenish_pool()
                self._poll_inflight()
                if created:
                    consecutive_failures = 0
                elif consecutive_failures > 0:
                    pass
            except Exception as e:
                print(f"orchestrator: pool manager error: {e}")

            with self._lock:
                needed = (
                    min(len(self._sessions) + self.pool_size, self.max_containers)
                    - len(self._warm_pool)
                    - len(self._inflight)
                    - len(self._sessions)
                )
            if needed > 0 and not created:
                consecutive_failures += 1
            else:
                consecutive_failures = 0

            delay = min(self.poll_interval * (2 ** min(consecutive_failures, 4)), 30)
            if consecutive_failures > 0 and consecutive_failures % 5 == 1:
                print(
                    f"orchestrator: create failing, backoff {delay}s (consecutive failures: {consecutive_failures})"
                )
            time.sleep(delay)

    # ------------------------------------------------------------------
    # Session management
    # ------------------------------------------------------------------

    def get_or_assign(
        self, session_id: str, is_connected=None
    ) -> tuple[ContainerRecord | None, str | None]:
        """Assign a container to a session, blocking up to 60s if pool is empty.
        Returns (record, error_message)."""
        with self._condition:
            if session_id in self._sessions:
                return self._sessions[session_id], None

            if len(self._sessions) >= self.max_containers:
                return None, f"at capacity ({self.max_containers} sessions)"

            deadline = time.time() + 60
            while not self._warm_pool:
                remaining = deadline - time.time()
                if remaining <= 0:
                    return None, "no containers available (timed out after 60s)"
                if is_connected and not is_connected():
                    print(
                        f"orchestrator: client disconnected while waiting for session {session_id}"
                    )
                    return None, "client disconnected"
                self._condition.wait(timeout=min(remaining, 2))

            rec = self._warm_pool.pop(0)
            rec.status = "assigned"
            rec.assigned_at = time.time()
            self._sessions[session_id] = rec
            print(f"orchestrator: assigned {rec.name} to session {session_id}")
            return rec, None

    def cleanup_session(self, session_id: str) -> ContainerRecord | None:
        """Release a session and schedule its container for deletion."""
        with self._lock:
            rec = self._sessions.pop(session_id, None)
        if rec:
            rec.status = "deleting"
            self._close_http_client(rec.id)
            print(f"orchestrator: cleaning up {rec.name} for session {session_id}")
            threading.Thread(
                target=self._delete_container, args=(rec.id,), daemon=True
            ).start()
        return rec

    def cleanup_all(self) -> dict:
        """Delete every tracked container and clear all state."""
        with self._lock:
            all_recs = (
                list(self._warm_pool)
                + list(self._inflight)
                + list(self._sessions.values())
            )
            self._warm_pool.clear()
            self._inflight.clear()
            self._sessions.clear()
            self._failed.clear()
        for rec in all_recs:
            self._close_http_client(rec.id)
        deleted = []
        skipped = []
        for rec in all_recs:
            status_code, remote = self._api_request("GET", f"/api/containers/{rec.id}")
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
            self._delete_container(rec.id)
            deleted.append(rec.name)
        return {"deleted": deleted, "skipped": skipped, "count": len(deleted)}

    def finish(self) -> dict:
        """Stop replenishing, delete all containers."""
        self._shutting_down = True
        print("orchestrator: finishing — deleting all containers and shutting down")
        with self._lock:
            all_recs = (
                list(self._warm_pool)
                + list(self._inflight)
                + list(self._sessions.values())
            )
            self._warm_pool.clear()
            self._inflight.clear()
            self._sessions.clear()
            self._failed.clear()
        for rec in all_recs:
            self._close_http_client(rec.id)
        deleted = []
        for rec in all_recs:
            status_code, remote = self._api_request("GET", f"/api/containers/{rec.id}")
            if status_code != 200 or remote is None:
                continue
            if remote.get("name", "") != rec.name:
                continue
            print(f"orchestrator: deleting {rec.name} ({rec.id}) — verified")
            self._delete_container(rec.id)
            deleted.append(rec.name)
        return {"status": "finished", "deleted": deleted, "count": len(deleted)}

    # ------------------------------------------------------------------
    # Container proxy
    # ------------------------------------------------------------------

    def _proxy(self, rec: ContainerRecord, path: str, body: bytes) -> tuple[int, bytes]:
        """Proxy a request to a container's api-server over attested HTTPS."""
        url = f"https://{rec.domain}{path}"
        client = self._get_http_client(rec)
        try:
            resp = client.post(
                url,
                content=body,
                headers={"Content-Type": "application/json"},
                timeout=35,
            )
            return resp.status_code, resp.content
        except httpx.HTTPStatusError as e:
            return e.response.status_code, e.response.content
        except httpx.HTTPError as e:
            return 502, json.dumps({"error": f"container unavailable: {e}"}).encode()

    # ------------------------------------------------------------------
    # High-level operations
    # ------------------------------------------------------------------

    def exec_command(self, session_id: str, command: str) -> dict:
        """Execute a command in the session's container. Returns parsed response."""
        rec, err = self.get_or_assign(session_id)
        if rec is None:
            return {"error": err}
        status, raw = self._proxy(
            rec, "/exec", json.dumps({"command": command}).encode()
        )
        try:
            return json.loads(raw)
        except Exception:
            return {
                "error": f"proxy returned status {status}",
                "raw": raw.decode(errors="replace"),
            }

    def read_file(self, session_id: str, path: str) -> str:
        """Read a file from the session's container, returning text content."""
        rec, err = self.get_or_assign(session_id)
        if rec is None:
            raise RuntimeError(err)
        _status, raw = self._proxy(
            rec, "/read", json.dumps({"path": path}).encode()
        )
        result = json.loads(raw)
        if "error" in result:
            raise RuntimeError(result["error"])
        return base64.b64decode(result["contents"]).decode("utf-8")

    def write_file(self, session_id: str, path: str, content: str) -> dict:
        """Write text content to a file on the session's container."""
        encoded = base64.b64encode(content.encode("utf-8")).decode("ascii")
        rec, err = self.get_or_assign(session_id)
        if rec is None:
            return {"error": err}
        status, raw = self._proxy(
            rec,
            "/write",
            json.dumps({"path": path, "contents": encoded}).encode(),
        )
        try:
            return json.loads(raw)
        except Exception:
            return {"error": f"proxy returned status {status}"}

    def file_exists(self, session_id: str, path: str) -> bool:
        """Check if a file exists on the session's container."""
        try:
            self.read_file(session_id, path)
            return True
        except RuntimeError:
            return False

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def health_info(self) -> dict:
        with self._lock:
            return {
                "status": "ok",
                "warm_pool": len(self._warm_pool),
                "inflight": len(self._inflight),
                "sessions": len(self._sessions),
                "pool_target": self.pool_size,
                "max_containers": self.max_containers,
            }

    def metrics_info(self) -> dict:
        now = time.time()
        with self._lock:

            def _rec(r, session_id=None):
                d = {
                    "id": r.id,
                    "name": r.name,
                    "status": r.status,
                    "uptime": round(now - r.created_at),
                    "ssh_port": r.ssh_port,
                }
                if session_id:
                    d["session_id"] = session_id
                    d["active_time"] = (
                        round(now - r.assigned_at) if r.assigned_at else 0
                    )
                return d

            return {
                "warm_pool": [_rec(r) for r in self._warm_pool],
                "inflight": [_rec(r) for r in self._inflight],
                "sessions": [_rec(r, sid) for sid, r in self._sessions.items()],
                "failed": [_rec(r) for r in self._failed],
                "fail_count": self._fail_count,
                "api_errors": self._api_errors,
                "pool_target": self.pool_size,
                "max_containers": self.max_containers,
            }
