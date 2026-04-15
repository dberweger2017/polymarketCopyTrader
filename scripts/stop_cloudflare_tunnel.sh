#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="$ROOT_DIR/.cloudflared.pid"
URL_FILE="$ROOT_DIR/public_url.txt"

if [[ ! -f "$PID_FILE" ]]; then
  echo "No tunnel pid file found."
  rm -f "$URL_FILE"
  exit 0
fi

pid="$(cat "$PID_FILE" 2>/dev/null || true)"
if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
  kill "$pid"
fi

rm -f "$PID_FILE" "$URL_FILE"
echo "Cloudflare tunnel stopped."
