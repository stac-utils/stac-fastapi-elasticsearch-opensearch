#!/bin/bash
set -e

apt-get update -y
apt-get install -y build-essential curl openssh-client
mkdir -p /root/.ssh/ && chmod 0700 /root/.ssh
echo "${GITLAB_SSH_KEY}" > /root/.ssh/id_ed25519 && chmod 0600 /root/.ssh/id_ed25519
echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config

pip install --upgrade pip setuptools wheel
pip install ./stac_fastapi/core
pip install ./stac_fastapi/sfeos_helpers
pip install ./stac_fastapi/opensearch[dev,server]

echo "Waiting for OpenSearch"
timeout 100 bash -c 'until curl -f http://opensearch:9200/_cluster/health; do sleep 5; done'
echo "\nOpenSearch is ready"
