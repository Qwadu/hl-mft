#!/usr/bin/env bash
# Sync the repo to the VPS and (re)start the stack.
#   HOST=root@1.2.3.4 bash deploy/deploy.sh            # bot only
#   HOST=root@1.2.3.4 PROFILE=grafana bash deploy/deploy.sh   # bot + alloy -> Grafana Cloud
# Requires: rsync + ssh locally; deploy/setup_vps.sh already run on the host.
# The server keeps its own config.yaml and .env in $REMOTE_DIR; they are never overwritten from here.
set -euo pipefail

HOST="${HOST:?set HOST=user@ip}"
REMOTE_DIR="${REMOTE_DIR:-/opt/hl-mft}"
PROFILE="${PROFILE:-}"
HERE="$(cd "$(dirname "$0")/.." && pwd)"

ssh "$HOST" "mkdir -p '$REMOTE_DIR/data'"
rsync -az --delete \
  --exclude '.git' --exclude '.venv' --exclude 'data' --exclude '__pycache__' \
  --exclude '.env' --exclude 'config.yaml' --exclude '*.db' \
  "$HERE/" "$HOST:$REMOTE_DIR/"

ssh "$HOST" bash -s <<EOF
set -euo pipefail
cd '$REMOTE_DIR'
[ -f .env ] || { cp .env.example .env; echo ">> created .env from example — fill it in: $REMOTE_DIR/.env"; }
[ -f config.yaml ] || { cp config.example.yaml config.yaml; echo ">> created config.yaml (mode: record)"; }
docker compose build --pull bot
if [ -n "$PROFILE" ]; then docker compose --profile "$PROFILE" up -d; else docker compose up -d; fi
docker compose ps
EOF

echo "dashboard: ssh -L 8080:127.0.0.1:8080 $HOST  ->  http://localhost:8080/?token=<DASHBOARD_TOKEN>"
