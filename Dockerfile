FROM cloudflare/cloudflared:latest AS cloudflared

FROM node:22-alpine AS frontend-builder

WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm install
COPY frontend ./
RUN npm run build

FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt ./
COPY scripts/requirements-market-recorder.txt ./scripts/requirements-market-recorder.txt
RUN pip install --no-cache-dir -r requirements.txt -r scripts/requirements-market-recorder.txt

COPY --from=cloudflared /usr/local/bin/cloudflared /usr/local/bin/cloudflared
COPY bot_core.py main.py options_dashboard.py server.py docker-entrypoint.sh ./
COPY scripts ./scripts
COPY --from=frontend-builder /app/frontend/dist ./frontend/dist
RUN chmod +x /app/docker-entrypoint.sh

EXPOSE 8000

CMD ["/app/docker-entrypoint.sh"]
