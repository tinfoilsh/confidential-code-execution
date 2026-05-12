#!/usr/bin/env bash
# Live view of orchestrator metrics. Refreshes every second.
# Filters to orchestrator_* lines (drops Go runtime + process metrics).
#
# Usage: ./viz.sh [URL]   (default: http://localhost:7070)

set -euo pipefail
URL="${1:-http://localhost:7070}"
exec watch -n 1 "curl -sf ${URL}/metrics | grep -E '^(# HELP |# TYPE |orchestrator_)'"
