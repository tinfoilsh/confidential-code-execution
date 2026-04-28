"""Live CLI dashboard for the orchestrator. Polls /metrics every second."""

import json
import os
import sys
import time
import urllib.request

URL = os.environ.get("ORCHESTRATOR_URL", "http://localhost:7070")

# ANSI colors
RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"

GREEN = "\033[32m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
RED = "\033[31m"
WHITE = "\033[37m"
BG_GREEN = "\033[42m"
BG_YELLOW = "\033[43m"
BG_CYAN = "\033[46m"
BG_RED = "\033[41m"
BG_GRAY = "\033[100m"


def fmt_uptime(s: int) -> str:
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m{s % 60:02d}s"
    return f"{s // 3600}h{(s % 3600) // 60:02d}m"


def box(label: str, sub: str, bg: str, width: int = 30, extra: str = "") -> list[str]:
    """Render a single container as a box."""
    inner = width - 2
    top = f"{bg} {'─' * inner} {RESET}"
    mid = f"{bg} {BOLD}{label:<{inner}}{RESET}{bg} {RESET}"
    bot_text = f"{sub:<{inner}}"
    bot = f"{bg} {DIM}{bot_text}{RESET}{bg} {RESET}"
    lines = [top, mid, bot]
    if extra:
        ext_text = f"{extra:<{inner}}"
        lines.append(f"{bg} {DIM}{ext_text}{RESET}{bg} {RESET}")
    return lines


def render(data: dict) -> str:
    lines: list[str] = []

    lines.append("")
    lines.append(
        f"  {BOLD}Orchestrator Dashboard{RESET}  {DIM}(pool target: {data['pool_target']}, max: {data['max_containers']}){RESET}"
    )
    lines.append("")

    # --- Warm pool ---
    warm = data.get("warm_pool", [])
    lines.append(f"  {GREEN}{BOLD}WARM POOL{RESET} {DIM}({len(warm)}){RESET}")
    if warm:
        rows = _layout_boxes(warm, BG_GREEN, GREEN)
        lines.extend(rows)
    else:
        lines.append(f"    {DIM}(empty){RESET}")
    lines.append("")

    # --- Inflight ---
    inflight = data.get("inflight", [])
    lines.append(f"  {YELLOW}{BOLD}INFLIGHT{RESET} {DIM}({len(inflight)}){RESET}")
    if inflight:
        rows = _layout_boxes(inflight, BG_YELLOW, YELLOW)
        lines.extend(rows)
    else:
        lines.append(f"    {DIM}(none){RESET}")
    lines.append("")

    # --- Sessions ---
    sessions = data.get("sessions", [])
    lines.append(f"  {CYAN}{BOLD}ACTIVE SESSIONS{RESET} {DIM}({len(sessions)}){RESET}")
    if sessions:
        rows = _layout_boxes(sessions, BG_CYAN, CYAN, show_session=True)
        lines.extend(rows)
    else:
        lines.append(f"    {DIM}(none){RESET}")
    lines.append("")

    # --- Failures ---
    failed = data.get("failed", [])
    fail_count = data.get("fail_count", 0)
    api_errors = data.get("api_errors", 0)
    lines.append(
        f"  {RED}{BOLD}FAILURES{RESET} {DIM}(total: {fail_count}, api errors: {api_errors}){RESET}"
    )
    if failed:
        rows = _layout_boxes(failed, BG_RED, RED)
        lines.extend(rows)
    elif fail_count == 0 and api_errors == 0:
        lines.append(f"    {DIM}(none){RESET}")
    lines.append("")

    return "\n".join(lines)


def _layout_boxes(
    containers: list[dict], bg: str, fg: str, show_session: bool = False
) -> list[str]:
    """Lay out boxes side-by-side, wrapping at terminal width."""
    box_width = 44 if show_session else 30
    cols = max(1, (os.get_terminal_size().columns - 4) // (box_width + 2))
    rows: list[str] = []

    for i in range(0, len(containers), cols):
        chunk = containers[i : i + cols]
        rendered = []
        for c in chunk:
            label = c["name"]
            sub = f"{c['id'][:8]}  {fmt_uptime(c['uptime'])}"
            extra = ""
            if show_session and c.get("session_id"):
                active = c.get("active_time", 0)
                sub = f"sid:{c['session_id'][:12]}  active {fmt_uptime(active)}"
                ssh_port = c.get("ssh_port", 0)
                if ssh_port:
                    extra = f"ssh -p {ssh_port} root@console.tinfoil.sh"
            rendered.append(box(label, sub, bg, box_width, extra))

        # zip the lines of each box together (may be 3 or 4 lines)
        max_lines = max(len(b) for b in rendered)
        for row_idx in range(max_lines):
            blank = " " * box_width
            line = "    " + "  ".join(
                b[row_idx] if row_idx < len(b) else blank for b in rendered
            )
            rows.append(line)
        rows.append("")

    return rows


def fetch_metrics() -> dict | None:
    try:
        req = urllib.request.Request(f"{URL}/metrics")
        with urllib.request.urlopen(req, timeout=2) as resp:
            return json.loads(resp.read())
    except Exception:
        return None


def clear_screen():
    os.system("clear")


if __name__ == "__main__":
    print(f"Connecting to {URL}/metrics ...")
    while True:
        data = fetch_metrics()
        clear_screen()
        if data:
            sys.stdout.write(render(data))
        else:
            sys.stdout.write(
                f"\n  {RED}{BOLD}Cannot reach orchestrator at {URL}{RESET}\n"
            )
            sys.stdout.write(f"  {DIM}Retrying...{RESET}\n")
        sys.stdout.flush()
        time.sleep(1)
