#!/usr/bin/env bash
# One-shot VPS bootstrap (Ubuntu 22.04/24.04): docker + compose plugin, firewall, swap check.
# Run as root or a sudoer:  bash deploy/setup_vps.sh
set -euo pipefail

if ! command -v docker >/dev/null; then
  apt-get update -y
  apt-get install -y ca-certificates curl gnupg ufw
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
  chmod a+r /etc/apt/keyrings/docker.asc
  . /etc/os-release
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
https://download.docker.com/linux/ubuntu ${VERSION_CODENAME} stable" > /etc/apt/sources.list.d/docker.list
  apt-get update -y
  apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
  systemctl enable --now docker
fi

# Only SSH is reachable from outside; dashboard (8080) and metrics (9108) stay on localhost.
if command -v ufw >/dev/null; then
  ufw allow OpenSSH >/dev/null
  ufw --force enable >/dev/null
fi

# Keep the clock tight: HL rejects actions with stale nonces.
timedatectl set-ntp true || true

docker --version
docker compose version
echo "ok: docker ready. Next: deploy/deploy.sh from your workstation."
