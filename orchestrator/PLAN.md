# Orchestrator for Tinfoil Code Execution Containers

## Context

There's a working code execution setup: an api-server + executor running inside a Tinfoil enclave, deployed via the Tinfoil controlplane API. The orchestrator manages a warm pool of these executor containers and routes requests by session ID, so multiple clients each get their own isolated sandbox.

## Reference files

These files show the patterns to follow and the APIs to call:

- `/Users/dmccanns/Desktop/Tinfoil/code-execution/code-container/executor/main.py` — the executor container. Exposes `POST /exec` (runs bash), `POST /read` (reads file, returns base64), `POST /write` (writes base64 to file), `GET /health`. Uses `http.server.BaseHTTPRequestHandler`. This is the upstream that the orchestrator proxies to.
- `/Users/dmccanns/Desktop/Tinfoil/code-execution/code-container/executor/Dockerfile` — executor Dockerfile (for reference when containerizing orchestrator later).
- `/Users/dmccanns/Desktop/Tinfoil/code-execution/code-container/api-server/main.py` — thin HTTP proxy that forwards `/exec`, `/read`, `/write` to the executor on localhost:9000. Shows the `urllib.request` proxying pattern to reuse.
- `/Users/dmccanns/Desktop/Tinfoil/code-execution/code-container/api-server/Dockerfile` — api-server Dockerfile.
- `/Users/dmccanns/Desktop/Tinfoil/controlplane/handlers/containers_handler.go` — the controlplane's container CRUD handler. `CreateContainer` starts at line 216. Shows the full request/response JSON shape.
- `/Users/dmccanns/Desktop/Tinfoil/controlplane/constants/containers.go` — container status values: `pending`, `deploying`, `started`, `ready`, `failed`, `stopping`, `stopped`.
- `/Users/dmccanns/Desktop/Tinfoil/controlplane/services/tinfoild_service.go` — how the controlplane talks to tinfoild (for understanding the deploy flow).
- `/Users/dmccanns/Desktop/Tinfoil/controlplane/README.md` — full API reference for all controlplane endpoints.

## File to create

`/Users/dmccanns/Desktop/Tinfoil/code-execution/code-container/orchestrator/main.py`

Single file, **stdlib only** (no dependencies), matching the pattern of executor/main.py and api-server/main.py.

## Controlplane API reference

Base URL: `https://api.tinfoil.sh`
Auth: `Authorization: Bearer <ADMIN_API_KEY>` (admin key has org context bound to it, no separate org header needed)

### POST /api/containers — Create container

```json
{
  "name": "exec-a1b2c3d4",
  "repo": "tinfoilsh/confidential-code-execution",
  "tag": "v0.0.2",
  "debug": true
}
```

Response (201): full container JSON including `id` (UUID string), `domain` (HTTPS hostname), `status` ("deploying").

Domain pattern for debug mode: `<name>.debug.tinfoil.containers.tinfoil.dev`

### GET /api/containers/:id — Poll status

Returns container JSON. Status transitions: `pending` → `deploying` → `started` → `ready` (or `failed`). The controlplane polls tinfoild in real time on each GET, so this is the polling endpoint.

### DELETE /api/containers/:id — Delete container

Returns 204 No Content. Cleans up DNS, billing, and tinfoild deployment.

### GET /api/containers — List all containers

Returns JSON array of container objects.

## Design

### Data model

```python
@dataclass
class ContainerRecord:
    id: str          # Tinfoil container UUID from POST response
    name: str        # "exec-a1b2c3d4"
    domain: str      # from POST response "domain" field
    status: str      # deploying | ready | assigned | deleting
    created_at: float
```

### Shared state (thread-safe)

- `_warm_pool: list[ContainerRecord]` — ready containers, not yet assigned
- `_inflight: list[ContainerRecord]` — containers currently deploying
- `_sessions: dict[str, ContainerRecord]` — sessionId → assigned container
- Protected by `threading.Condition` (wrapping a `threading.Lock`) for wait/notify on pool empty

### HTTP endpoints

| Endpoint        | Body                                                     | Behavior                                                                                   |
| --------------- | -------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `POST /exec`    | `{"sessionId": "...", "command": "..."}`                 | Assign container if new session, proxy to executor's `/exec`                               |
| `POST /read`    | `{"sessionId": "...", "path": "..."}`                    | Same, proxy to `/read`                                                                     |
| `POST /write`   | `{"sessionId": "...", "path": "...", "contents": "..."}` | Same, proxy to `/write`                                                                    |
| `POST /cleanup` | `{"sessionId": "..."}`                                   | Release session, delete container in background thread                                     |
| `GET /health`   | —                                                        | Returns `{"status": "ok", "warm_pool": N, "inflight": N, "sessions": N, "pool_target": N}` |

**Important**: strip `sessionId` from the body before forwarding to the executor — the executor doesn't know about sessions.

### Warm pool manager (single background daemon thread)

Runs every `POLL_INTERVAL` seconds (default 2s):

1. Count `len(warm_pool) + len(inflight)`. If below `POOL_SIZE` (3), create new containers via `POST /api/containers`. Add to `_inflight`.
2. Poll all inflight containers via `GET /api/containers/:id`.
3. If status is `ready`: move from `_inflight` to `_warm_pool`, call `condition.notify_all()` to wake any waiting request handlers.
4. If status is `failed`: remove from `_inflight` (next cycle creates a replacement).

**Critical**: container creation (HTTP POST) and status polling (HTTP GET) happen **outside** the lock to avoid blocking request handlers during network I/O. Only list mutations happen under the lock.

### Session assignment

On first request for a new sessionId:

1. Acquire the condition lock
2. Check if sessionId already in `_sessions` — if so, return it
3. If `_warm_pool` is empty, `condition.wait(timeout=60)` — blocks until a container becomes ready
4. Pop from warm pool (FIFO / `pop(0)` — oldest first), set status to `assigned`
5. Store in `_sessions[sessionId]`
6. Return the record

If 60s timeout expires with no container available, return HTTP 503.

### Container naming

```python
name = f"exec-{uuid.uuid4().hex[:8]}"  # e.g. "exec-a1b2c3d4"
```

Lowercase alphanumeric + hyphens, satisfies Tinfoil naming constraints. 8 hex chars = ~4 billion combinations, collision is negligible for a pool of 3.

### Configuration (all from environment variables)

| Variable        | Required | Default                                 | Description                                |
| --------------- | -------- | --------------------------------------- | ------------------------------------------ |
| `ADMIN_API_KEY` | Yes      | —                                       | Admin API key for controlplane             |
| `POOL_SIZE`     | No       | `3`                                     | Number of warm containers to maintain      |
| `PORT`          | No       | `7000`                                  | Port for the orchestrator HTTP server      |
| `POLL_INTERVAL` | No       | `2`                                     | Seconds between pool manager cycles        |
| `CONFIG_REPO`   | No       | `tinfoilsh/confidential-code-execution` | GitHub repo for container config           |
| `CONFIG_TAG`    | No       | `v0.0.2`                                | Git tag to deploy                          |
| `DEBUG_MODE`    | No       | `true`                                  | Whether to deploy containers in debug mode |

### Threading model

Use `ThreadingHTTPServer` (stdlib, Python 3.7+) instead of `HTTPServer`. This spawns a thread per request so a slow `/exec` call or pool-empty wait doesn't block other requests.

### Module structure (single file)

```python
# stdlib imports
import dataclasses, json, os, threading, time, uuid
import urllib.error, urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# configuration (from env)
# data model (@dataclass ContainerRecord)
# shared state (lock, condition, pools, sessions)
# Tinfoil API helpers (_api_request, _create_container, _poll_container, _delete_container)
# pool management (_replenish_pool, _pool_manager_loop)
# session management (_get_or_assign, _cleanup_session)
# proxying (_proxy)
# HTTP handler (OrchestratorHandler)
# entrypoint (start background thread, then serve_forever)
```

## Verification

```bash
# Terminal 1: start orchestrator
cd /Users/dmccanns/Desktop/Tinfoil/code-execution/code-container
export ADMIN_API_KEY="admin_qsOcJEnbiZ42TKEr7BfMkQtFYCOSxDuczE7LIsoxfw7PYbvv"
python orchestrator/main.py
```

```bash
# Terminal 2: test

# 1. Check pool is filling
curl http://localhost:7000/health

# 2. Wait ~1-3 min for containers to become ready, then:
curl -X POST http://localhost:7000/exec \
  -H "Content-Type: application/json" \
  -d '{"sessionId": "test1", "command": "echo hello"}'

# 3. Same session, read a file
curl -X POST http://localhost:7000/read \
  -H "Content-Type: application/json" \
  -d '{"sessionId": "test1", "path": "/etc/hostname"}'

# 4. Different session (gets a different container)
curl -X POST http://localhost:7000/exec \
  -H "Content-Type: application/json" \
  -d '{"sessionId": "test2", "command": "whoami"}'

# 5. Cleanup a session
curl -X POST http://localhost:7000/cleanup \
  -H "Content-Type: application/json" \
  -d '{"sessionId": "test1"}'

# 6. Pool should replenish
curl http://localhost:7000/health
```
