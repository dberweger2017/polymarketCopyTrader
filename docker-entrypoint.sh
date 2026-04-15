#!/bin/sh
set -eu

PUBLIC_URL_FILE="/app/public_url.txt"
TUNNEL_LOG_FILE="/app/.cloudflared.log"

UVICORN_PID=""
CLOUDFLARED_PID=""

cleanup() {
  rm -f "$PUBLIC_URL_FILE"
  if [ -n "${CLOUDFLARED_PID}" ] && kill -0 "$CLOUDFLARED_PID" 2>/dev/null; then
    kill "$CLOUDFLARED_PID" 2>/dev/null || true
  fi
  if [ -n "${UVICORN_PID}" ] && kill -0 "$UVICORN_PID" 2>/dev/null; then
    kill "$UVICORN_PID" 2>/dev/null || true
  fi
}

trap cleanup INT TERM EXIT

rm -f "$PUBLIC_URL_FILE" "$TUNNEL_LOG_FILE"

uvicorn server:app --host 0.0.0.0 --port 8000 &
UVICORN_PID="$!"

if [ "${ENABLE_CLOUDFLARE_TUNNEL:-1}" = "1" ]; then
  cloudflared tunnel --no-autoupdate --url http://127.0.0.1:8000 >"$TUNNEL_LOG_FILE" 2>&1 &
  CLOUDFLARED_PID="$!"

  i=0
  while [ "$i" -lt 45 ]; do
    if [ -f "$TUNNEL_LOG_FILE" ]; then
      TUNNEL_URL="$(grep -o 'https://[-A-Za-z0-9]*\.trycloudflare\.com' "$TUNNEL_LOG_FILE" | head -n 1 || true)"
      if [ -n "$TUNNEL_URL" ]; then
        printf '%s\n' "$TUNNEL_URL" >"$PUBLIC_URL_FILE"
        break
      fi
    fi

    if [ -n "$CLOUDFLARED_PID" ] && ! kill -0 "$CLOUDFLARED_PID" 2>/dev/null; then
      break
    fi

    i=$((i + 1))
    sleep 1
  done
fi

wait "$UVICORN_PID"
