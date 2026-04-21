# Code Execution

## Orchestrator

Spins up multiple environment containers. Manages them. Routes requests through to them.

**Threading approach w/ ThreadingHTTPServer**: only uses stdlib. Threads cost more memory, could be annoying at >1k concurrent

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
