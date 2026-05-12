#!/usr/bin/env bash
# Live view of orchestrator metrics. Refreshes every second.
# Filters to orchestrator_* lines (drops Go runtime + process metrics).
#
# Usage: ./viz.sh [URL]   (default: http://localhost:7070)

set -euo pipefail
URL="${1:-http://localhost:7070}"
printf '\033[2J'  # clear once on start
while true; do
  printf '\033[H\033[J'  # cursor home + erase to end of screen (no scrollback)
  curl -sf "${URL}/metrics" | grep -E '^orchestrator_(warm_pool_size|sessions_active|attestation_failures_total|containers_deleted_total) '
  sleep 1
done
