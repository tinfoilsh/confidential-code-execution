# Code Execution

## Orchestrator

Manages a warm pool of executor containers via the Tinfoil controlplane API. Routes requests by session ID so each client gets an isolated sandbox. The primary tool interface is MCP (`POST /mcp`).

Written in Go. Stdlib only + `github.com/tinfoilsh/verifier` for enclave attestation.

### Run

```bash
export ADMIN_API_KEY="..."
go run .
```

Or build a binary:

```bash
go build .
./confidential-code-execution
```

### Environment variables

| Variable         | Default                                | Notes                              |
| ---------------- | -------------------------------------- | ---------------------------------- |
| `ADMIN_API_KEY`  | _(required)_                           | Tinfoil controlplane bearer token  |
| `POOL_SIZE`      | `3`                                    | Target warm pool size              |
| `MAX_CONTAINERS` | `10`                                   | Hard cap on concurrent containers  |
| `PORT`           | `7070`                                 | Orchestrator listen port           |
| `POLL_INTERVAL`  | `2`                                    | Seconds between controlplane polls |
| `CONFIG_REPO`    | `tinfoilsh/code-execution-environment` | Image repo                         |

|  
| `DEBUG_MODE` | `true` | Pass `debug=true` to controlplane (enables SSH, modifies measurement) |
| `VERIFY_ATTESTATION` | `false` | Verify enclave attestation + pin TLS public key. Requires `DEBUG_MODE=false`. |
| `SKIP_JWT_VALIDATION` | `false` | Local dev only — accept `tools/call` without a Clerk JWT. Never set in prod. |

### API

All tool access is through the single MCP endpoint. The session is identified by the per-chat secret in the `X-Code-Execution-Access-Token` header.

```
POST /mcp
Headers: X-Code-Execution-Access-Token: <token>
Body: JSON-RPC 2.0

Methods:
  initialize       — handshake
  tools/list       — list available tools
  tools/call       — invoke a tool (params: {name, arguments})

Tools: bash, view, present, str_replace, create, insert
```

Admin endpoints:

```
GET  /health        — pool counts
GET  /metrics       — full container details (used by viz.py)
POST /cleanup       {"codeExecutionAccessToken": "abc"}  — release a single session
POST /delete-all    — delete all tracked containers (with name verification)
POST /finish        — delete all containers and shut down the server
```

### Visualizer

```bash
python viz.py
```

Polls `/metrics` every second. Shows warm pool (green), inflight (yellow), active sessions (cyan), and failures (red).

### Agent

`agent.py` defines our code-execution tools (bash & text editor) and runs a simple agent loop against Tinfoil inference to exercise the orchestrator end-to-end.

```bash
export TF_API_KEY="..."
python agent.py
```

## Environment Container

_in code-execution-environment repo_

- **api-server** (port 8000) — HTTP API exposed via the Tinfoil shim. Proxies requests to the executor.
- **executor** (port 9000) — Runs bash commands and serves file read/write.

### API

```
POST /exec   {"command": "echo hello"}
             → {"stdout": "hello\n", "stderr": "", "exit_code": 0}

POST /read   {"path": "/workspace/file.txt"}
             → {"path": "...", "contents": "<base64>"}

POST /write  {"path": "...", "contents": "<base64>"}
             → {"path": "...", "size": 42}

GET  /health → {"status": "ok"}
```
