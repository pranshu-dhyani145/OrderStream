#!/usr/bin/env bash
# One-time setup for a fresh Ubuntu 24.04 EC2 instance (t3.small or bigger).
# Usage: curl -fsSL https://raw.githubusercontent.com/<you>/orderstream/main/scripts/ec2-setup.sh | bash -s <repo-url>
set -euo pipefail
REPO_URL="${1:?usage: ec2-setup.sh <public-repo-url>}"

# Docker + compose plugin
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker "$USER"

# 1 GB swap as a safety net for Kafka's JVM on small instances
if ! swapon --show | grep -q /swapfile; then
  sudo fallocate -l 1G /swapfile
  sudo chmod 600 /swapfile
  sudo mkswap /swapfile
  sudo swapon /swapfile
  echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
fi

# Get the code and start the stack (sudo because the docker group isn't active in this shell yet)
[ -d ~/orderstream ] || git clone "$REPO_URL" ~/orderstream
cd ~/orderstream
sudo docker compose up -d --build

echo "Done. Log out and back in so 'docker' works without sudo, then open http://<EC2_PUBLIC_IP>:8000"
