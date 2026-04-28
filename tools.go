package main

import (
	"fmt"
	"strings"
)

type ToolSchema struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}

// ToolHandler executes a tool call and returns a text result for MCP.
type ToolHandler func(m *Manager, sessionID string, args map[string]any) (string, error)

var Tools = []ToolSchema{
	{
		Name:        "bash",
		Description: "Execute a bash command in a sandboxed container. Returns stdout, stderr, and exit code.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"command": map[string]any{"type": "string", "description": "The bash command to execute"},
			},
			"required": []string{"command"},
		},
	},
	{
		Name:        "view",
		Description: "View a file's contents with line numbers. Returns the full file or a specific line range.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"path": map[string]any{"type": "string", "description": "Absolute or workspace-relative path to the file"},
				"view_range": map[string]any{
					"type":        "array",
					"items":       map[string]any{"type": "integer"},
					"minItems":    2,
					"maxItems":    2,
					"description": "Optional [start_line, end_line] (1-indexed, inclusive). Omit to view the entire file.",
				},
			},
			"required": []string{"path"},
		},
	},
	{
		Name:        "str_replace",
		Description: "Replace an exact string occurrence in a file. The old_str must appear exactly once in the file.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"path":    map[string]any{"type": "string", "description": "Absolute or workspace-relative path to the file"},
				"old_str": map[string]any{"type": "string", "description": "The exact string to find (must be unique in the file)"},
				"new_str": map[string]any{"type": "string", "description": "The replacement string"},
			},
			"required": []string{"path", "old_str", "new_str"},
		},
	},
	{
		Name:        "create",
		Description: "Create a new file with the given contents. Fails if the file already exists.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"path":      map[string]any{"type": "string", "description": "Absolute or workspace-relative path for the new file"},
				"file_text": map[string]any{"type": "string", "description": "The full contents of the file to create"},
			},
			"required": []string{"path", "file_text"},
		},
	},
	{
		Name:        "insert",
		Description: "Insert text after a specific line number in a file. Use line 0 to insert at the beginning.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"path":        map[string]any{"type": "string", "description": "Absolute or workspace-relative path to the file"},
				"line_number": map[string]any{"type": "integer", "description": "Insert after this line (0 = beginning of file, 1 = after first line)"},
				"text":        map[string]any{"type": "string", "description": "The text to insert"},
			},
			"required": []string{"path", "line_number", "text"},
		},
	},
}

var ToolHandlers = map[string]ToolHandler{
	"bash":        handleBash,
	"view":        handleView,
	"str_replace": handleStrReplace,
	"create":      handleCreate,
	"insert":      handleInsert,
}

func argString(args map[string]any, key string) (string, error) {
	v, ok := args[key]
	if !ok {
		return "", fmt.Errorf("missing argument: %s", key)
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("argument %s must be a string", key)
	}
	return s, nil
}

func argInt(args map[string]any, key string) (int, error) {
	v, ok := args[key]
	if !ok {
		return 0, fmt.Errorf("missing argument: %s", key)
	}
	switch n := v.(type) {
	case float64:
		return int(n), nil
	case int:
		return n, nil
	}
	return 0, fmt.Errorf("argument %s must be an integer", key)
}

func handleBash(m *Manager, sessionID string, args map[string]any) (string, error) {
	cmd, err := argString(args, "command")
	if err != nil {
		return "", err
	}
	resp := m.ExecCommand(sessionID, cmd)
	if e, ok := resp["error"].(string); ok && e != "" {
		return "", fmt.Errorf("%s", e)
	}
	stdout, _ := resp["stdout"].(string)
	stderr, _ := resp["stderr"].(string)
	exit, _ := resp["exit_code"].(float64)
	var b strings.Builder
	if stdout != "" {
		fmt.Fprintf(&b, "stdout:\n%s\n", stdout)
	}
	if stderr != "" {
		fmt.Fprintf(&b, "stderr:\n%s\n", stderr)
	}
	fmt.Fprintf(&b, "exit_code: %d", int(exit))
	return b.String(), nil
}

func handleView(m *Manager, sessionID string, args map[string]any) (string, error) {
	path, err := argString(args, "path")
	if err != nil {
		return "", err
	}
	content, err := m.ReadFile(sessionID, path)
	if err != nil {
		return "", err
	}
	lines := strings.Split(content, "\n")

	start, end := 1, len(lines)
	if r, ok := args["view_range"].([]any); ok && len(r) == 2 {
		if s, ok := r[0].(float64); ok {
			start = int(s)
		}
		if e, ok := r[1].(float64); ok {
			end = int(e)
		}
	}
	if start < 1 {
		start = 1
	}
	if end > len(lines) {
		end = len(lines)
	}
	if start > end {
		return "", fmt.Errorf("invalid range: start %d > end %d", start, end)
	}

	var b strings.Builder
	for i := start; i <= end; i++ {
		fmt.Fprintf(&b, "%6d\t%s\n", i, lines[i-1])
	}
	return b.String(), nil
}

func handleStrReplace(m *Manager, sessionID string, args map[string]any) (string, error) {
	path, err := argString(args, "path")
	if err != nil {
		return "", err
	}
	oldStr, err := argString(args, "old_str")
	if err != nil {
		return "", err
	}
	newStr, err := argString(args, "new_str")
	if err != nil {
		return "", err
	}
	content, err := m.ReadFile(sessionID, path)
	if err != nil {
		return "", err
	}
	count := strings.Count(content, oldStr)
	if count == 0 {
		return "", fmt.Errorf("old_str not found in %s", path)
	}
	if count > 1 {
		return "", fmt.Errorf("old_str appears %d times in %s — must be unique", count, path)
	}
	updated := strings.Replace(content, oldStr, newStr, 1)
	resp := m.WriteFile(sessionID, path, updated)
	if e, ok := resp["error"].(string); ok && e != "" {
		return "", fmt.Errorf("%s", e)
	}
	return fmt.Sprintf("Replaced 1 occurrence in %s", path), nil
}

func handleCreate(m *Manager, sessionID string, args map[string]any) (string, error) {
	path, err := argString(args, "path")
	if err != nil {
		return "", err
	}
	text, err := argString(args, "file_text")
	if err != nil {
		return "", err
	}
	if m.FileExists(sessionID, path) {
		return "", fmt.Errorf("file already exists: %s", path)
	}
	resp := m.WriteFile(sessionID, path, text)
	if e, ok := resp["error"].(string); ok && e != "" {
		return "", fmt.Errorf("%s", e)
	}
	return fmt.Sprintf("Created %s", path), nil
}

func handleInsert(m *Manager, sessionID string, args map[string]any) (string, error) {
	path, err := argString(args, "path")
	if err != nil {
		return "", err
	}
	lineNum, err := argInt(args, "line_number")
	if err != nil {
		return "", err
	}
	text, err := argString(args, "text")
	if err != nil {
		return "", err
	}
	content, err := m.ReadFile(sessionID, path)
	if err != nil {
		return "", err
	}
	lines := strings.Split(content, "\n")
	if lineNum < 0 || lineNum > len(lines) {
		return "", fmt.Errorf("line_number %d out of range (file has %d lines)", lineNum, len(lines))
	}
	out := append([]string{}, lines[:lineNum]...)
	out = append(out, text)
	out = append(out, lines[lineNum:]...)
	resp := m.WriteFile(sessionID, path, strings.Join(out, "\n"))
	if e, ok := resp["error"].(string); ok && e != "" {
		return "", fmt.Errorf("%s", e)
	}
	return fmt.Sprintf("Inserted text after line %d in %s", lineNum, path), nil
}
