# Code Execution

## Orchestrator

Manages a warm pool of executor containers via the Tinfoil controlplane API. Routes requests by session ID so each client gets an isolated sandbox.

Stdlib only (+ `dotenv`). Uses `ThreadingHTTPServer` — one thread per request so slow `/exec` calls or pool-empty waits don't block others.

### Run

```bash
export ADMIN_API_KEY="..."
python orchestrator/main.py
```

Config via env vars:

| Variable         | Default                                 | Description                         |
| ---------------- | --------------------------------------- | ----------------------------------- |
| `ADMIN_API_KEY`  | required                                | Admin API key for controlplane      |
| `POOL_SIZE`      | `3`                                     | Warm containers to maintain         |
| `MAX_CONTAINERS` | `10`                                    | Max total containers                |
| `PORT`           | `7070`                                  | Orchestrator port                   |
| `POLL_INTERVAL`  | `2`                                     | Seconds between pool manager cycles |
| `CONFIG_REPO`    | `tinfoilsh/confidential-code-execution` | Container repo                      |
| `CONFIG_TAG`     | `v0.0.3`                                | Container tag                       |
| `DEBUG_MODE`     | `true`                                  | Deploy in debug mode                |

### API

```
POST /exec
{"sessionId": "abc", "command": "echo hello"}
-> {"stdout": "hello\n", "stderr": "", "exit_code": 0}

POST /read
{"sessionId": "abc", "path": "/workspace/file.txt"}
-> {"path": "/workspace/file.txt", "contents": "<base64>"}

POST /write
{"sessionId": "abc", "path": "/workspace/file.txt", "contents": "<base64>"}
-> {"path": "/workspace/file.txt", "size": 42}

POST /cleanup
{"sessionId": "abc"}
-> {"status": "cleaned up", "container": "daniel-exec-a1b2c3d4"}

POST /delete-all   — delete all tracked containers (with name verification)
POST /finish       — delete all containers and shut down the server

GET /health        — pool counts
GET /metrics       — full container details (used by viz.py)
```

### Dashboard

```bash
python orchestrator/viz.py
```

Polls `/metrics` every second. Shows warm pool (green), inflight (yellow), active sessions (cyan), and failures (red).

## Environment Container

- **api-server** (port 8000) — HTTP API exposed via the Tinfoil shim. Proxies requests to the executor.
- **executor** (port 9000) — Runs bash comamnds w/ subprocess run

### API

```
POST /exec
{"command": "echo hello"}

Response:
{"stdout": "hello\n", "stderr": "", "exit_code": 0}
```

```
GET /health
{"status": "ok"}
```
