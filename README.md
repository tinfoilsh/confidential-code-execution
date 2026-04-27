# Code Execution

## Orchestrator

Manages a warm pool of executor containers via the Tinfoil controlplane API. Routes requests by session ID so each client gets an isolated sandbox.

Stdlib only (+ `dotenv`). Uses `ThreadingHTTPServer` — one thread per request so slow `/exec` calls or pool-empty waits don't block others.

### Run

```bash
export ADMIN_API_KEY="..."
python orchestrator/main.py
```

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

### Visualizer

```bash
python orchestrator/viz.py
```

Polls `/metrics` every second. Shows warm pool (green), inflight (yellow), active sessions (cyan), and failures (red).

### Agent

`agent.py` is a minimal implementation that defines our code execution tools - bash & text editor - and usese tinfoils inference to make a very simple agent loop to test out code execution.

## Test

```bash
docker exec -it code-executor bash
```

```bash
curl -X POST http://localhost:8000/exec \
  -H "Content-Type: application/json" \
  -d '{
    "command": "echo hello > /workspace/hello.txt\ncat /workspace/hello.txt"
  }'
```

Should see:
`{"stdout": "hello\n", "stderr": "", "exit_code": 0}`

## Environment Container

_in code-execution-environment repo_

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
