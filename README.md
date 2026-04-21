# Code Execution Container

Two-container setup for running bash commands inside a Tinfoil enclave.

## Architecture

- **api-server** (port 8000) — HTTP API exposed via the Tinfoil shim. Proxies requests to the executor.
- **executor** (port 9000) — Runs bash commands in an isolated container. Not directly exposed.

## API

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

## Building

```bash
docker build -t ghcr.io/<org>/code-api-server:latest ./api-server
docker build -t ghcr.io/<org>/code-executor:latest ./executor
docker push ghcr.io/<org>/code-api-server:latest
docker push ghcr.io/<org>/code-executor:latest
```
