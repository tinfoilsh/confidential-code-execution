"""
MCP-based agent: talks to the orchestrator exclusively via POST /mcp.
"""

import json
import os
import urllib.request
import uuid
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from openai.types.chat import ChatCompletionToolParam
from openai.types.chat.chat_completion_message_function_tool_call import (
    ChatCompletionMessageFunctionToolCall,
)
from tinfoil import TinfoilAI

load_dotenv()

client = TinfoilAI(api_key=os.environ["TF_API_KEY"])

MODEL = "kimi-k2-6"
MCP_URL = os.environ.get("MCP_URL", "http://localhost:7070/mcp")
MAX_TURNS = 10


# ---------------------------------------------------------------------------
# MCP client helpers
# ---------------------------------------------------------------------------
def _mcp_request(method: str, params: dict | None = None, session_id: str = "") -> dict:
    """Send a JSON-RPC request to the MCP endpoint and return the result."""
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
    }
    if params is not None:
        body["params"] = params

    headers = {"Content-Type": "application/json"}
    if session_id:
        headers["X-Session-Id"] = session_id

    req = urllib.request.Request(
        MCP_URL,
        data=json.dumps(body).encode(),
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = json.loads(resp.read())

    if "error" in data:
        raise RuntimeError(f"MCP error: {data['error']}")
    return data.get("result", {})


def _list_tools() -> list[ChatCompletionToolParam]:
    """Fetch tools from the MCP server and convert to OpenAI format."""
    result = _mcp_request("tools/list")
    mcp_tools = result.get("tools", [])
    openai_tools: list[ChatCompletionToolParam] = []
    for t in mcp_tools:
        openai_tools.append(
            {
                "type": "function",
                "function": {
                    "name": t["name"],
                    "description": t.get("description", ""),
                    "parameters": t.get("inputSchema", {}),
                },
            }
        )
    return openai_tools


def _call_tool(name: str, args: dict, session_id: str) -> str:
    """Call a tool via MCP and return the text content."""
    result = _mcp_request(
        "tools/call",
        {"name": name, "arguments": args},
        session_id=session_id,
    )
    # Extract text from MCP content array
    content_parts = result.get("content", [])
    texts = [p.get("text", "") for p in content_parts if p.get("type") == "text"]
    return "\n".join(texts)


# ---------------------------------------------------------------------------
# Agent loop
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = """You are a helpful assistant with access to a sandboxed container. You have these tools:

- bash: Run bash commands. Use for installing packages, running scripts, etc.
- view: Read a file with line numbers. Use view_range for large files.
- str_replace: Edit a file by replacing an exact unique string with new text.
- create: Create a new file (fails if it already exists).
- insert: Insert text after a specific line number (0 = beginning of file).

Prefer the file tools (view, str_replace, create, insert) over bash for file operations.
When you have the answer, respond directly without calling any more tools."""


def log(lines: list[str], msg: str) -> None:
    """Print and record a log line."""
    print(msg)
    lines.append(msg)


def run_agent(user_message: str) -> str:
    session_id = uuid.uuid4().hex[:12]

    lines: list[str] = []
    tools = _list_tools()

    messages: list = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_message},
    ]

    log(lines, "=" * 60)
    log(lines, f"User: {user_message}")
    log(lines, f"Model: {MODEL}")
    log(lines, f"Session: {session_id}")
    log(lines, f"MCP endpoint: {MCP_URL}")
    log(lines, "=" * 60)

    final_content = ""

    for turn in range(MAX_TURNS):
        log(lines, f"\n{'=' * 60}")
        log(lines, f"Turn {turn + 1}")
        log(lines, "=" * 60)

        response = client.chat.completions.create(
            model=MODEL,
            messages=messages,
            tools=tools,
            tool_choice="auto",
        )

        message = response.choices[0].message
        usage = response.usage
        if usage:
            log(
                lines,
                f"[tokens] prompt={usage.prompt_tokens} completion={usage.completion_tokens} total={usage.total_tokens}",
            )

        # No tool calls — we're done
        if not message.tool_calls:
            log(lines, f"\n[assistant] {message.content}")
            final_content = message.content or ""
            break

        # Log assistant thinking + tool calls
        if message.content:
            log(lines, f"[thinking] {message.content.strip()}")

        log(lines, f"[tool_calls] {len(message.tool_calls)} call(s)")
        for i, tc in enumerate(message.tool_calls):
            if isinstance(tc, ChatCompletionMessageFunctionToolCall):
                log(
                    lines,
                    f"  [{i}] {tc.function.name}({tc.function.arguments}) id={tc.id}",
                )

        # Append assistant message with tool calls
        messages.append(message)

        # Execute all tool calls via MCP
        for tool_call in message.tool_calls:
            if not isinstance(tool_call, ChatCompletionMessageFunctionToolCall):
                continue

            args = json.loads(tool_call.function.arguments)
            log(lines, f"\n[exec] {tool_call.function.name}({json.dumps(args)})")

            result = _call_tool(tool_call.function.name, args, session_id)
            log(lines, f"[result]\n{result}")

            messages.append(
                {
                    "role": "tool",
                    "content": result,
                    "tool_call_id": tool_call.id,
                }
            )
    else:
        final_content = "Max turns reached."
        log(lines, f"\n[error] {final_content}")

    # Write log file
    log_dir = Path(__file__).parent / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    log_file.write_text("\n".join(lines) + "\n")
    print(f"\n[log saved to {log_file}]")

    return final_content


if __name__ == "__main__":
    result = run_agent(
        "Try out the tools you have available. Make a few fun files. Do something artistic. Explore!"
    )
    print(f"\n{'=' * 60}")
    print(f"Final answer:\n{result}")
