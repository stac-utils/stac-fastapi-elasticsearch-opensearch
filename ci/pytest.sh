#!/bin/bash
set -e

echo "Running OpenSearch tests..."
pytest -sv --timeout=10 stac_fastapi/opensearch/tests/

