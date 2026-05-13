#!/usr/bin/env bash
# Live view of orchestrator metrics. Refreshes every second.
#
# Usage: ./viz.sh [URL]   (default: http://localhost:7070)

set -euo pipefail
URL="${1:-http://localhost:7070}"

# ANSI
RST=$'\033[0m'; BOLD=$'\033[1m'; DIM=$'\033[2m'
CYAN=$'\033[36m'; MAG=$'\033[35m'; YEL=$'\033[33m'
GRN=$'\033[32m'; RED=$'\033[31m'; BLU=$'\033[34m'
PINK=$'\033[38;5;218m'

val() { awk -v k="^$1 " '$0 ~ k {print $2; exit}' <<<"$2"; }

# Color a value: dim when 0, otherwise the given color (bold).
paint() {
  local v="$1" color="$2"
  if [[ "${v:-?}" == "0" ]]; then
    printf '%s%s%s' "$DIM" "${v:-?}" "$RST"
  else
    printf '%s%s%s%s' "$BOLD" "$color" "${v:-?}" "$RST"
  fi
}

printf '\033[2J'  # clear once on start
while true; do
  metrics=$(curl -sf "${URL}/metrics" 2>/dev/null || true)

  inflight=$(val orchestrator_inflight_size "$metrics")
  warm=$(val orchestrator_warm_pool_size "$metrics")
  active=$(val orchestrator_sessions_active "$metrics")
  failed=$(val orchestrator_attestation_failures_total "$metrics")
  deleted=$(val orchestrator_containers_deleted_total "$metrics")

  # Build full frame in memory, then write atomically (no flicker).
  frame="$(printf '\033[H\033[J')"
  frame+="${BOLD}${CYAN}╔══ orchestrator ══╗${RST}
  ${MAG}inflight${RST} $(paint "$inflight" "$PINK")
  ${MAG}warm    ${RST} $(paint "$warm" "$GRN")
  ${MAG}active  ${RST} $(paint "$active" "$BLU")
  ${MAG}failed  ${RST} $(paint "$failed" "$RED")
  ${MAG}deleted ${RST} $(paint "$deleted" "$YEL")
${DIM}$(date '+%H:%M:%S')  •  ${URL}${RST}
"
  printf '%s' "$frame"
  sleep 1
done
