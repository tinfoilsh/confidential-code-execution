# Code Execution

## MCP tools

- `bash` — run a bash command in the container; returns stdout, stderr, exit code.
- `view` — read a file with line numbers, optionally a `[start, end]` range.
- `present` — render a file inline in the chat as a syntax-highlighted code block (the user sees it directly).
- `str_replace` — replace one exact occurrence of `old_str` with `new_str` in a file.
- `create` — create a new file with given contents; fails if it already exists.
- `insert` — insert text after a given line number in a file.

## Flow

### Tool call

```mermaid
sequenceDiagram
    participant C as Client
    participant M as mcp.go
    participant Mgr as Manager
    participant Cont as Container
    box External
    participant CP as Controlplane
    participant B as Buckets
    end

    C->>M: POST /mcp (tools/call)<br/>headers: api_key, accessToken, [enc_key]
    M->>CP: POST /api/shim/validate-key
    CP-->>M: 200
    M->>Mgr: dispatch(ctx, accessToken, args)

    alt session hit
        Mgr->>Mgr: reuse cached *Container from m.sessions
    else session miss
        Mgr->>Cont: GET /health (popped warm)
        Cont-->>Mgr: 200
        opt enc_key + bearer present
            Mgr->>B: GET /items/{accessToken}
            B-->>Mgr: plaintext tar
            Mgr->>Cont: POST /restore (tar)
        end
        Mgr->>Mgr: m.sessions[accessToken] = container
    end

    Mgr->>Cont: POST /exec | /read | /write
    Cont-->>Mgr: result
    Mgr-->>M: text
    M-->>C: JSON-RPC response
```

### Background loops

```mermaid
flowchart LR
    subgraph Pool[Pool Manager]
        direction TB
        P1[tick: POLL_INTERVAL]
        P1 --> P2{warm + inflight + sessions<br/>below target?}
        P2 -->|yes| P3[create container]
        P1 --> P4[poll inflight]
        P4 -->|ready| P5[→ warm pool]
    end

    subgraph Health[Health Checker]
        direction TB
        H1[tick: HEALTH_CHECK_INTERVAL]
        H1 --> H2[GET container /health]
        H2 -->|200| H3[reset HealthFailures]
        H2 -->|fail| H4[HealthFailures++]
        H4 -->|≥ MAX_HEALTH_FAILURES| H5[evict + delete]
    end

    subgraph Evict[Idle Evictor]
        direction TB
        E1[tick: EvictionPoll]
        E1 --> E2{LastActivity > IdleTimeout?}
        E2 -->|yes| E3[POST container /snapshot]
        E3 --> E4[PUT to buckets]
        E4 --> E5[delete container]
    end

    P3 -.-> CP[(Controlplane)]
    P4 -.-> CP
    H5 -.-> CP
    E5 -.-> CP
    E4 -.-> B[(Buckets)]

    classDef ext stroke-dasharray:5 5
    class CP,B ext
```

### Container lifecycle

```mermaid
stateDiagram-v2
    [*] --> deploying: createContainer
    deploying --> ready: controlplane status=ready
    deploying --> failed: deploy fail
    ready --> failed: /health fail × MAX_HEALTH_FAILURES<br/>(periodic or pre-assign)
    ready --> assigning: GetOrAssign + /health ok
    assigning --> assigned: restore-on-assign done
    assigned --> assigned: tools/call (refresh LastActivity)
    assigned --> deleting: idle evict or shutdown<br/>(snapshot to buckets first)
    deleting --> [*]
    failed --> [*]
```

## Manager

Manages a warm pool of executor containers via the Tinfoil controlplane API. Routes requests by session ID so each client gets an isolated sandbox. The primary tool interface is MCP (`POST /mcp`).

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

| Variable           | Default                                | Notes                              |
| ------------------ | -------------------------------------- | ---------------------------------- |
| `ADMIN_API_KEY`    | _(required)_                           | Tinfoil controlplane bearer token  |
| `POOL_SIZE`        | `3`                                    | Target warm pool size              |
| `MAX_CONTAINERS`   | `10`                                   | Hard cap on concurrent containers  |
| `PORT`             | `7070`                                 | manager listen port                |
| `POLL_INTERVAL`    | `2`                                    | Seconds between controlplane polls |
| `ENVIRONMENT_REPO` | `tinfoilsh/code-execution-environment` | Source repo for the executor image |
| `ENVIRONMENT_TAG`  | `v0.0.9`                               | Image tag to deploy                |

|  
| `DEV_SKIP_ATTESTATION` | `false` | Local dev only — skip enclave attestation + TLS pinning. Never set in prod. |
| `DEV_BYPASS_AUTH` | `false` | Local dev only — skip api_key validation. Never set in prod. |
| `HEALTH_CHECK_INTERVAL` | `15` | Seconds between `/health` probes of warm containers. |
| `MAX_HEALTH_FAILURES` | `3` | Consecutive `/health` failures before a warm container is destroyed and replaced. |
| `SHUTDOWN_DEADLINE` | `25` | Seconds spent snapshotting sessions on SIGTERM before bulk-delete. Tune per platform grace period. |

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

Other endpoints:

```
GET /metrics — full container details (used by viz.py)
```

On `SIGINT`/`SIGTERM` the manager snapshots every active session to
buckets, deletes every container it owns on controlplane, then exits —
so a deploy or local `ctrl-c` doesn't leak containers or session state.

### Visualizer

```bash
python viz.py
```

Polls `/metrics` every second. Shows warm pool (green), inflight (yellow), active sessions (cyan), and failures (red).

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
