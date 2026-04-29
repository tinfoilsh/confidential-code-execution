package main

import (
	"fmt"
	"path/filepath"
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
		Name:        "present",
		Description: "Render a file's contents to the user as a syntax-highlighted code block in the chat. The model receives only an acknowledgement; the file's contents are not echoed back as a tool result. Use this to show the user a file (logs, source, generated artifact) without re-typing it. Same arguments as view.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"path": map[string]any{"type": "string", "description": "Absolute or workspace-relative path to the file"},
				"view_range": map[string]any{
					"type":        "array",
					"items":       map[string]any{"type": "integer"},
					"minItems":    2,
					"maxItems":    2,
					"description": "Optional [start_line, end_line] (1-indexed, inclusive). Omit to present the entire file.",
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
	"present":     handlePresent,
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

// readFileLineRange reads `path` from the session and resolves the
// optional view_range argument. Returns the file's lines plus the
// 1-indexed [start, end] window the caller should display. When
// view_range is omitted the window covers the whole file.
func readFileLineRange(m *Manager, sessionID string, args map[string]any) (path string, lines []string, start, end int, err error) {
	path, err = argString(args, "path")
	if err != nil {
		return "", nil, 0, 0, err
	}
	content, err := m.ReadFile(sessionID, path)
	if err != nil {
		return "", nil, 0, 0, err
	}
	lines = strings.Split(content, "\n")
	start, end = 1, len(lines)
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
		return "", nil, 0, 0, fmt.Errorf("invalid range: start %d > end %d", start, end)
	}
	return path, lines, start, end, nil
}

func handleView(m *Manager, sessionID string, args map[string]any) (string, error) {
	_, lines, start, end, err := readFileLineRange(m, sessionID, args)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	for i := start; i <= end; i++ {
		fmt.Fprintf(&b, "%6d\t%s\n", i, lines[i-1])
	}
	return b.String(), nil
}

// extensionToLanguage maps file extensions to the language tag used in
// markdown fenced code blocks. Unknown extensions return "" so the fence
// renders without a language hint.
var extensionToLanguage = map[string]string{
	".py":         "python",
	".js":         "javascript",
	".jsx":        "jsx",
	".ts":         "typescript",
	".tsx":        "tsx",
	".go":         "go",
	".rs":         "rust",
	".java":       "java",
	".kt":         "kotlin",
	".swift":      "swift",
	".c":          "c",
	".h":          "c",
	".cc":         "cpp",
	".cpp":        "cpp",
	".cxx":        "cpp",
	".hpp":        "cpp",
	".cs":         "csharp",
	".rb":         "ruby",
	".php":        "php",
	".sh":         "bash",
	".bash":       "bash",
	".zsh":        "bash",
	".fish":       "bash",
	".ps1":        "powershell",
	".sql":        "sql",
	".html":       "html",
	".htm":        "html",
	".css":        "css",
	".scss":       "scss",
	".sass":       "sass",
	".less":       "less",
	".json":       "json",
	".jsonc":      "json",
	".yaml":       "yaml",
	".yml":        "yaml",
	".toml":       "toml",
	".xml":        "xml",
	".md":         "markdown",
	".markdown":   "markdown",
	".tex":        "latex",
	".r":          "r",
	".lua":        "lua",
	".pl":         "perl",
	".scala":      "scala",
	".clj":        "clojure",
	".ex":         "elixir",
	".exs":        "elixir",
	".erl":        "erlang",
	".hs":         "haskell",
	".dart":       "dart",
	".vim":        "vim",
	".dockerfile": "dockerfile",
	".makefile":   "makefile",
	".cmake":      "cmake",
	".graphql":    "graphql",
	".proto":      "protobuf",
}

// inferLanguage picks a markdown code-fence language from a path. Falls back
// to filename matches (Dockerfile, Makefile) before giving up.
func inferLanguage(path string) string {
	ext := strings.ToLower(filepath.Ext(path))
	if lang, ok := extensionToLanguage[ext]; ok {
		return lang
	}
	base := strings.ToLower(filepath.Base(path))
	switch base {
	case "dockerfile":
		return "dockerfile"
	case "makefile", "gnumakefile":
		return "makefile"
	case "cmakelists.txt":
		return "cmake"
	}
	return ""
}

// handlePresent reads a file and returns it as a fenced markdown code
// block with language inferred from extension. The router emits this
// output as inline assistant content so the user sees the file rendered
// directly in the chat alongside the model's tool result.
func handlePresent(m *Manager, sessionID string, args map[string]any) (string, error) {
	path, lines, start, end, err := readFileLineRange(m, sessionID, args)
	if err != nil {
		return "", err
	}
	body := strings.Join(lines[start-1:end], "\n")
	// Use a longer fence than any backtick run inside the file so nested
	// triple-backtick code (common in markdown files) doesn't terminate it.
	fence := longestFence(body)
	return fmt.Sprintf("%s%s\n%s\n%s", fence, inferLanguage(path), body, fence), nil
}

// longestFence returns a backtick fence longer than any run of backticks
// inside body, so embedding fenced code blocks won't break out.
func longestFence(body string) string {
	longest := 0
	run := 0
	for _, r := range body {
		if r == '`' {
			run++
			if run > longest {
				longest = run
			}
		} else {
			run = 0
		}
	}
	n := longest + 1
	if n < 3 {
		n = 3
	}
	return strings.Repeat("`", n)
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
