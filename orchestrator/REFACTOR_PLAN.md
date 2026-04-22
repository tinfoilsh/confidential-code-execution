# Refactor: Consolidate orchestrator into MCP-first architecture

## Context

The orchestrator currently has a monolithic `main.py` exposing raw `/exec`, `/read`, `/write` HTTP endpoints, a separate `mcp.py` that proxies to those endpoints, and an `agent.py` that also calls those endpoints directly. We want to mirror the confidential-websearch architecture (`/Users/dmccanns/Desktop/Tinfoil/confidential-websearch/`): one process, `POST /mcp` as the primary tool interface, clean separation of concerns.

### Reference architecture (confidential-websearch)

- `/Users/dmccanns/Desktop/Tinfoil/confidential-websearch/main.go` — HTTP server lifecycle, route setup, creates MCP handler
- `/Users/dmccanns/Desktop/Tinfoil/confidential-websearch/mcp_server.go` — MCP server factory, tool registration (thin)
- `/Users/dmccanns/Desktop/Tinfoil/confidential-websearch/handlers.go` — Tool handler implementations + input validation
- `/Users/dmccanns/Desktop/Tinfoil/confidential-websearch/tools/service.go` — Business logic layer (search/fetch providers, safety checks)

### Current files being refactored

- `/Users/dmccanns/Desktop/Tinfoil/code-execution/code-container/orchestrator/main.py` — Monolithic: HTTP server + pool management + session management + proxy + admin endpoints
- `/Users/dmccanns/Desktop/Tinfoil/code-execution/code-container/orchestrator/agent.py` — Test client with tool definitions and handlers calling orchestrator HTTP API directly
- `/Users/dmccanns/Desktop/Tinfoil/code-execution/code-container/orchestrator/mcp.py` — Separate MCP server that proxies tool calls to orchestrator HTTP API

### Key context: how the model router consumes MCP servers

- `/Users/dmccanns/Desktop/Tinfoil/confidential-model-router/toolruntime/runtime.go` — `connectToolSession()` opens MCP session, `ListTools()` discovers tools, `CallTool()` executes them
- `/Users/dmccanns/Desktop/Tinfoil/confidential-model-router/toolruntime/session_registry.go` — Maps tool names to MCP sessions
- `/Users/dmccanns/Desktop/Tinfoil/confidential-model-router/toolprofile/profile.go` — Tool profiles (currently only `WebSearch`)
- `/Users/dmccanns/Desktop/Tinfoil/confidential-model-router/toolcontext/context.go` — Headers the router sends: `X-Tinfoil-Tool-Request-Id`, `X-Tinfoil-Tool-Model`, etc.
- `/Users/dmccanns/Desktop/Tinfoil/confidential-model-router/local_testing.md` — How to run locally with `LOCAL_MCP_ENDPOINT_<MODEL>` overrides

## Target File Layout

```
/Users/dmccanns/Desktop/Tinfoil/code-execution/code-container/orchestrator/
├── main.py              # HTTP server + route setup + entrypoint
├── mcp.py               # MCP JSON-RPC handling (thin)
├── tools.py             # Tool schemas + handler functions
├── container_manager.py # ContainerManager class: pool, sessions, proxy
├── agent.py             # Simplified: MCP client + LLM loop
├── viz.py               # Unchanged
```

## What goes where

### `/Users/dmccanns/Desktop/Tinfoil/code-execution/code-container/orchestrator/container_manager.py`

Extract from current `main.py`. Wraps everything in a `ContainerManager` class (mirrors websearch's `tools.Service` struct).

- `ContainerRecord` dataclass (currently main.py lines 30-38)
- `ContainerManager` class encapsulating:
  - Config: admin_api_key, pool_size, max_containers, poll_interval, config_repo, config_tag, debug_mode
  - All shared state: \_lock, \_condition, \_warm_pool, \_inflight, \_sessions, \_failed, \_fail_count, \_api_errors, \_shutting_down
  - Controlplane API: `_api_request()`, `_create_container()`, `_poll_container()`, `_delete_container()` (currently main.py lines 61-130)
  - Pool management: `_replenish_pool()`, `_poll_inflight()`, `start_pool_manager()` (currently main.py lines 136-228)
  - Session management: `get_or_assign()`, `cleanup_session()`, `cleanup_all()`, `finish()` (currently main.py lines 234-275, 416-491)
  - Container proxy: `_proxy()` — forwards HTTPS requests to container's api-server (currently main.py lines 281-298)
  - High-level operations (new, consolidate from current mcp.py/agent.py helpers):
    - `exec_command(session_id, command) -> dict` — get_or_assign + proxy to /exec
    - `read_file(session_id, path) -> str` — get_or_assign + proxy to /read + base64 decode
    - `write_file(session_id, path, content) -> dict` — base64 encode + proxy to /write
    - `file_exists(session_id, path) -> bool` — try read, return True/False
  - Status: `health_info() -> dict`, `metrics_info() -> dict` (currently main.py lines 331-375)

### `/Users/dmccanns/Desktop/Tinfoil/code-execution/code-container/orchestrator/tools.py`

Consolidate tool schemas from current mcp.py and handler logic from current agent.py/mcp.py.

- `TOOLS` list — MCP-format schemas with name, description, inputSchema (currently in mcp.py lines 42-155)
- Handler functions, each with signature `(manager: ContainerManager, session_id: str, args: dict) -> str`:
  - `handle_bash()` — calls manager.exec_command()
  - `handle_view()` — calls manager.read_file(), formats with line numbers
  - `handle_str_replace()` — calls manager.read_file(), validates unique match, calls manager.write_file()
  - `handle_create()` — calls manager.file_exists() check, calls manager.write_file()
  - `handle_insert()` — calls manager.read_file(), manipulates lines, calls manager.write_file()
- `TOOL_HANDLERS` dict mapping tool name → handler function

### `/Users/dmccanns/Desktop/Tinfoil/code-execution/code-container/orchestrator/mcp.py`

Slim down to just MCP protocol handling. Mirrors websearch's `mcp_server.go`.

- Protocol constants: `PROTOCOL_VERSION = "2025-03-26"`, `SERVER_NAME`, `SERVER_VERSION`
- `create_mcp_handler(manager: ContainerManager)` → returns an HTTP handler class (or function)
- JSON-RPC routing:
  - `initialize` → return capabilities + server info
  - `notifications/initialized` → 202 acknowledge
  - `tools/list` → return TOOLS from tools.py
  - `tools/call` → extract session_id from headers, dispatch to TOOL_HANDLERS
- Session ID extraction: `X-Tinfoil-Tool-Request-Id` (from router) or `X-Session-Id` (from agent.py), fallback to `"mcp-default"`

### `/Users/dmccanns/Desktop/Tinfoil/code-execution/code-container/orchestrator/main.py`

Rewrite as thin server entry point. Mirrors websearch's `main.go`.

- Load config from env vars (ADMIN_API_KEY, POOL_SIZE, MAX_CONTAINERS, PORT, POLL_INTERVAL, CONFIG_REPO, CONFIG_TAG, DEBUG_MODE)
- Create `ContainerManager` instance
- Start pool manager background thread
- Single HTTP server (`ThreadingHTTPServer`) with handler that routes:
  - `POST /mcp` → delegate to MCP handler from mcp.py
  - `GET /health` → manager.health_info()
  - `GET /metrics` → manager.metrics_info()
  - `POST /cleanup` → manager.cleanup_session(sessionId from body)
  - `POST /delete-all` → manager.cleanup_all()
  - `POST /finish` → manager.finish() + server.shutdown()
- **Remove**: `/exec`, `/read`, `/write` endpoints (replaced by MCP tools)

### `/Users/dmccanns/Desktop/Tinfoil/code-execution/code-container/orchestrator/agent.py`

Simplify to pure MCP client + LLM loop. No more direct orchestrator HTTP calls.

- Config: MCP_URL (default `http://localhost:7070/mcp`), MODEL, MAX_TURNS
- MCP client helper: `_mcp_request(method, params, session_id)` → POST JSON-RPC to MCP_URL with `X-Session-Id` header
- `_list_tools()` → calls `tools/list`, converts MCP schemas to OpenAI ChatCompletionToolParam format
- `_call_tool(name, args, session_id)` → calls `tools/call`, returns text content
- `run_agent(user_message)` → generates session_id, lists tools, runs LLM loop, calls `/cleanup` at end
- Keep: TinfoilAI client, logging, system prompt

### `/Users/dmccanns/Desktop/Tinfoil/code-execution/code-container/orchestrator/viz.py`

Unchanged — it polls `GET /metrics` which stays on the same server and port.

## Verification

1. Start orchestrator: `cd /Users/dmccanns/Desktop/Tinfoil/code-execution/code-container/orchestrator && python main.py`
2. Smoke test MCP with curl:

   ```bash
   # initialize
   curl -sS -X POST http://localhost:7070/mcp -H 'Content-Type: application/json' \
     -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}'

   # list tools
   curl -sS -X POST http://localhost:7070/mcp -H 'Content-Type: application/json' \
     -d '{"jsonrpc":"2.0","id":2,"method":"tools/list"}'

   # call bash (needs containers)
   curl -sS -X POST http://localhost:7070/mcp -H 'Content-Type: application/json' \
     -H 'X-Session-Id: test-1' \
     -d '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"bash","arguments":{"command":"echo hello"}}}'
   ```

3. Run `python agent.py` to verify LLM loop works through MCP
4. Run `python viz.py` to verify metrics still work
