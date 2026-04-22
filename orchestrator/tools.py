"""
Tool schemas and handler functions for the code execution MCP server.

Each handler takes (manager, session_id, args) and returns a string result.
"""

from container_manager import ContainerManager

# ---------------------------------------------------------------------------
# MCP tool definitions
# ---------------------------------------------------------------------------
TOOLS = [
    {
        "name": "bash",
        "description": (
            "Execute a bash command in a sandboxed container. "
            "Returns stdout, stderr, and exit code."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "command": {
                    "type": "string",
                    "description": "The bash command to execute",
                },
            },
            "required": ["command"],
        },
    },
    {
        "name": "view",
        "description": (
            "View a file's contents with line numbers. "
            "Returns the full file or a specific line range."
        ),
        "inputSchema": {
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
                    "description": (
                        "Optional [start_line, end_line] (1-indexed, inclusive). "
                        "Omit to view the entire file."
                    ),
                },
            },
            "required": ["path"],
        },
    },
    {
        "name": "str_replace",
        "description": (
            "Replace an exact string occurrence in a file. "
            "The old_str must appear exactly once in the file."
        ),
        "inputSchema": {
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
    {
        "name": "create",
        "description": "Create a new file with the given contents. Fails if the file already exists.",
        "inputSchema": {
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
    {
        "name": "insert",
        "description": (
            "Insert text after a specific line number in a file. "
            "Use line 0 to insert at the beginning."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Absolute or workspace-relative path to the file",
                },
                "line_number": {
                    "type": "integer",
                    "description": (
                        "Insert after this line (0 = beginning of file, "
                        "1 = after first line)"
                    ),
                },
                "text": {
                    "type": "string",
                    "description": "The text to insert",
                },
            },
            "required": ["path", "line_number", "text"],
        },
    },
]


# ---------------------------------------------------------------------------
# Tool handlers
# ---------------------------------------------------------------------------
def handle_bash(manager: ContainerManager, session_id: str, args: dict) -> str:
    result = manager.exec_command(session_id, args["command"])
    if "error" in result:
        return f"error: {result['error']}"
    parts = []
    if result.get("stdout"):
        parts.append(f"stdout:\n{result['stdout']}")
    if result.get("stderr"):
        parts.append(f"stderr:\n{result['stderr']}")
    parts.append(f"exit_code: {result.get('exit_code', -1)}")
    return "\n".join(parts)


def handle_view(manager: ContainerManager, session_id: str, args: dict) -> str:
    content = manager.read_file(session_id, args["path"])
    lines = content.splitlines()
    view_range = args.get("view_range")

    if view_range:
        start, end = view_range
        start = max(1, start)
        end = min(len(lines), end)
        selected = lines[start - 1 : end]
        numbered = [f"{i:6d}\t{line}" for i, line in enumerate(selected, start=start)]
    else:
        numbered = [f"{i:6d}\t{line}" for i, line in enumerate(lines, start=1)]

    return "\n".join(numbered)


def handle_str_replace(manager: ContainerManager, session_id: str, args: dict) -> str:
    path = args["path"]
    old_str = args["old_str"]
    new_str = args["new_str"]

    content = manager.read_file(session_id, path)
    count = content.count(old_str)
    if count == 0:
        return f"error: old_str not found in {path}"
    if count > 1:
        return f"error: old_str appears {count} times in {path} (must be unique)"

    new_content = content.replace(old_str, new_str, 1)
    manager.write_file(session_id, path, new_content)

    new_lines = new_content.splitlines()
    replacement_first_line = new_str.splitlines()[0] if new_str else ""
    for i, line in enumerate(new_lines):
        if replacement_first_line and replacement_first_line in line:
            start = max(0, i - 2)
            end = min(len(new_lines), i + len(new_str.splitlines()) + 2)
            snippet = [f"{j+1:6d}\t{new_lines[j]}" for j in range(start, end)]
            return f"Replaced in {path}:\n" + "\n".join(snippet)

    return f"Replaced in {path}"


def handle_create(manager: ContainerManager, session_id: str, args: dict) -> str:
    path = args["path"]
    if manager.file_exists(session_id, path):
        return f"error: file already exists: {path}"
    result = manager.write_file(session_id, path, args["file_text"])
    if "error" in result:
        return f"error: {result['error']}"
    return f"Created {path} ({result.get('size', 0)} bytes)"


def handle_insert(manager: ContainerManager, session_id: str, args: dict) -> str:
    path = args["path"]
    line_number = args["line_number"]
    text = args["text"]

    content = manager.read_file(session_id, path)
    lines = content.splitlines(keepends=True)

    insert_lines = text.splitlines(keepends=True)
    if insert_lines and not insert_lines[-1].endswith("\n"):
        insert_lines[-1] += "\n"

    idx = max(0, min(line_number, len(lines)))
    new_lines = lines[:idx] + insert_lines + lines[idx:]
    new_content = "".join(new_lines)
    manager.write_file(session_id, path, new_content)

    start = max(0, idx - 1)
    end = min(len(new_lines), idx + len(insert_lines) + 1)
    display = new_content.splitlines()
    snippet = [f"{j+1:6d}\t{display[j]}" for j in range(start, end)]
    return f"Inserted at line {line_number} in {path}:\n" + "\n".join(snippet)


TOOL_HANDLERS = {
    "bash": handle_bash,
    "view": handle_view,
    "str_replace": handle_str_replace,
    "create": handle_create,
    "insert": handle_insert,
}
