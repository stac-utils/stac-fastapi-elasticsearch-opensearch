#!/bin/bash
set -e

echo "Waiting for OpenSearch"
timeout 100 bash -c 'until curl -f http://opensearch:9202/_cluster/health; do sleep 5; done'
echo "\nOpenSearch is ready"

echo "Installing test tools"
pip install --upgrade pip setuptools wheel
pip install ./stac_fastapi/core
pip install ./stac_fastapi/sfeos_helpers
pip install ./stac_fastapi/opensearch[dev,server]
pip install pytest-timeout

echo "Running OpenSearch tests"
pytest -v --timeout=10 --log-cli-level=ERROR stac_fastapi/tests/