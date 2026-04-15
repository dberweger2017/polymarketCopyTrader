#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="$ROOT_DIR/.cloudflared.pid"
LOG_FILE="$ROOT_DIR/.cloudflared.log"
URL_FILE="$ROOT_DIR/public_url.txt"

if [[ -f "$PID_FILE" ]]; then
  existing_pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "$existing_pid" ]] && kill -0 "$existing_pid" 2>/dev/null; then
    echo "Cloudflare tunnel already running with pid $existing_pid" >&2
    if [[ -f "$URL_FILE" ]]; then
      cat "$URL_FILE"
    fi
    exit 0
  fi
fi

rm -f "$LOG_FILE" "$URL_FILE"

cloudflared tunnel --no-autoupdate --url http://127.0.0.1:8000 >"$LOG_FILE" 2>&1 &
tunnel_pid=$!
echo "$tunnel_pid" >"$PID_FILE"

for _ in $(seq 1 45); do
  if ! kill -0 "$tunnel_pid" 2>/dev/null; then
    echo "cloudflared exited before producing a URL" >&2
    cat "$LOG_FILE" >&2 || true
    rm -f "$PID_FILE"
    exit 1
  fi

  tunnel_url="$(grep -o 'https://[-a-zA-Z0-9]*\.trycloudflare\.com' "$LOG_FILE" | head -n1 || true)"
  if [[ -n "$tunnel_url" ]]; then
    printf '%s\n' "$tunnel_url" >"$URL_FILE"
    echo "$tunnel_url"
    exit 0
  fi

  sleep 1
done

echo "Timed out waiting for Cloudflare tunnel URL" >&2
exit 1
