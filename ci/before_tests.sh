#!/bin/bash
set -e

apt-get update -y
apt-get install -y build-essential curl openssh-client
mkdir -p /root/.ssh/ && chmod 0700 /root/.ssh
echo "${GITLAB_SSH_KEY}" > /root/.ssh/id_ed25519 && chmod 0600 /root/.ssh/id_ed25519
echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config
