#!/bin/bash
set -e

echo "Running OpenSearch tests"
pytest -v --timeout=10 --log-cli-level=ERROR stac_fastapi/tests/
