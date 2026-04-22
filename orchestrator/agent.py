import base64
import json
import os
import urllib.error
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
ORCHESTRATOR_URL = os.environ.get("ORCHESTRATOR_URL", "http://localhost:7070")
MAX_TURNS = 10

# --- Tool definitions ---

tools: list[ChatCompletionToolParam] = [
    {
        "type": "function",
        "function": {
            "name": "bash",
            "description": "Execute a bash command in a sandboxed container. Returns stdout, stderr, and exit code.",
            "parameters": {
                "type": "object",
                "properties": {
                    "command": {
                        "type": "string",
                        "description": "The bash command to execute",
                    }
                },
                "required": ["command"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "view",
            "description": "View a file's contents with line numbers. Returns the full file or a specific line range.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Absolute or workspace-relative path to the file",
                    },
                    "view_range": {
                        "type": "array",
                        "items": {"type": "integer"},
                        "minItems": 2,
                        "maxItems": 2,
                        "description": "Optional [start_line, end_line] (1-indexed, inclusive). Omit to view the entire file.",
                    },
                },
                "required": ["path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "str_replace",
            "description": "Replace an exact string occurrence in a file. The old_str must appear exactly once in the file.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Absolute or workspace-relative path to the file",
                    },
                    "old_str": {
                        "type": "string",
                        "description": "The exact string to find (must be unique in the file)",
                    },
                    "new_str": {
                        "type": "string",
                        "description": "The replacement string",
                    },
                },
                "required": ["path", "old_str", "new_str"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "create",
            "description": "Create a new file with the given contents. Fails if the file already exists.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Absolute or workspace-relative path for the new file",
                    },
                    "file_text": {
                        "type": "string",
                        "description": "The full contents of the file to create",
                    },
                },
                "required": ["path", "file_text"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "insert",
            "description": "Insert text after a specific line number in a file. Use line 0 to insert at the beginning.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Absolute or workspace-relative path to the file",
                    },
                    "line_number": {
                        "type": "integer",
                        "description": "Insert after this line (0 = beginning of file, 1 = after first line)",
                    },
                    "text": {
                        "type": "string",
                        "description": "The text to insert",
                    },
                },
                "required": ["path", "line_number", "text"],
            },
        },
    },
]

# --- Container helpers ---


def _container_request(path: str, body: dict, timeout: int = 35) -> dict:
    """POST JSON to the orchestrator and return the parsed response."""
    body["sessionId"] = _session_id
    req = urllib.request.Request(
        f"{ORCHESTRATOR_URL}{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def _read_file(path: str) -> str:
    """Read a file from the container, returning its text content."""
    result = _container_request("/read", {"path": path})
    if "error" in result:
        raise RuntimeError(result["error"])
    return base64.b64decode(result["contents"]).decode("utf-8")


def _write_file(path: str, content: str) -> dict:
    """Write text content to a file on the container."""
    encoded = base64.b64encode(content.encode("utf-8")).decode("ascii")
    return _container_request("/write", {"path": path, "contents": encoded})


def _file_exists(path: str) -> bool:
    """Check if a file exists on the container."""
    try:
        _container_request("/read", {"path": path})
        return True
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
        raise


# --- Tool handlers ---

# Set per agent run — all tool calls in a run share the same session
_session_id: str = ""


def bash(command: str) -> str:
    """Send a command to the code execution container."""
    try:
        result = _container_request("/exec", {"command": command})
        parts = []
        if result.get("stdout"):
            parts.append(f"stdout:\n{result['stdout']}")
        if result.get("stderr"):
            parts.append(f"stderr:\n{result['stderr']}")
        parts.append(f"exit_code: {result.get('exit_code', -1)}")
        return "\n".join(parts)
    except Exception as e:
        return f"error: {e}"


def view(path: str, view_range: list[int] | None = None) -> str:
    """View a file with line numbers, optionally a specific range."""
    try:
        content = _read_file(path)
        lines = content.splitlines()

        if view_range:
            start, end = view_range
            # 1-indexed inclusive
            start = max(1, start)
            end = min(len(lines), end)
            selected = lines[start - 1 : end]
            numbered = [
                f"{i:6d}\t{line}" for i, line in enumerate(selected, start=start)
            ]
        else:
            numbered = [f"{i:6d}\t{line}" for i, line in enumerate(lines, start=1)]

        return "\n".join(numbered)
    except Exception as e:
        return f"error: {e}"


def str_replace(path: str, old_str: str, new_str: str) -> str:
    """Replace an exact unique string in a file."""
    try:
        content = _read_file(path)
        count = content.count(old_str)
        if count == 0:
            return f"error: old_str not found in {path}"
        if count > 1:
            return f"error: old_str appears {count} times in {path} (must be unique)"

        new_content = content.replace(old_str, new_str, 1)
        _write_file(path, new_content)

        # Show context around the replacement
        new_lines = new_content.splitlines()
        # Find where the replacement landed
        replacement_first_line = new_str.splitlines()[0] if new_str else ""
        for i, line in enumerate(new_lines):
            if replacement_first_line and replacement_first_line in line:
                start = max(0, i - 2)
                end = min(len(new_lines), i + len(new_str.splitlines()) + 2)
                snippet = [f"{j+1:6d}\t{new_lines[j]}" for j in range(start, end)]
                return f"Replaced in {path}:\n" + "\n".join(snippet)

        return f"Replaced in {path}"
    except Exception as e:
        return f"error: {e}"


def create(path: str, file_text: str) -> str:
    """Create a new file. Fails if it already exists."""
    try:
        if _file_exists(path):
            return f"error: file already exists: {path}"
        result = _write_file(path, file_text)
        return f"Created {path} ({result.get('size', 0)} bytes)"
    except Exception as e:
        return f"error: {e}"


def insert(path: str, line_number: int, text: str) -> str:
    """Insert text after a given line number (0 = beginning)."""
    try:
        content = _read_file(path)
        lines = content.splitlines(keepends=True)

        # Ensure text ends with newline for clean insertion
        insert_lines = text.splitlines(keepends=True)
        if insert_lines and not insert_lines[-1].endswith("\n"):
            insert_lines[-1] += "\n"

        idx = max(0, min(line_number, len(lines)))
        new_lines = lines[:idx] + insert_lines + lines[idx:]
        new_content = "".join(new_lines)
        _write_file(path, new_content)

        # Show context around insertion
        start = max(0, idx - 1)
        end = min(len(new_lines), idx + len(insert_lines) + 1)
        display = new_content.splitlines()
        snippet = [f"{j+1:6d}\t{display[j]}" for j in range(start, end)]
        return f"Inserted at line {line_number} in {path}:\n" + "\n".join(snippet)
    except Exception as e:
        return f"error: {e}"


TOOL_HANDLERS: dict[str, callable] = {  # type: ignore[type-arg]
    "bash": bash,
    "view": view,
    "str_replace": str_replace,
    "create": create,
    "insert": insert,
}

# --- Agent loop ---

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
    global _session_id
    _session_id = uuid.uuid4().hex[:12]

    lines: list[str] = []
    messages: list = [  # type: ignore[type-arg]
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_message},
    ]

    log(lines, "=" * 60)
    log(lines, f"User: {user_message}")
    log(lines, f"Model: {MODEL}")
    log(lines, f"Session: {_session_id}")
    log(lines, f"Orchestrator: {ORCHESTRATOR_URL}")
    log(lines, "=" * 60)

    final_content = ""

    for turn in range(MAX_TURNS):
        log(lines, f"\n{'=' * 60}")
        log(lines, f"Turn {turn + 1}")
        log(lines, "=" * 60)

        response = client.chat.completions.create(
            model=MODEL,
            messages=messages,  # type: ignore[arg-type]
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
        messages.append(message)  # type: ignore[arg-type]

        # Execute all tool calls
        for tool_call in message.tool_calls:
            if not isinstance(tool_call, ChatCompletionMessageFunctionToolCall):
                continue
            handler = TOOL_HANDLERS.get(tool_call.function.name)
            if not handler:
                log(lines, f"[skip] no handler for {tool_call.function.name}")
                continue

            args = json.loads(tool_call.function.arguments)
            log(lines, f"\n[exec] {tool_call.function.name}({json.dumps(args)})")

            result = handler(**args)
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
