#!/bin/bash
set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

APP_HOST="${APP_HOST:-0.0.0.0}"
APP_PORT="${APP_PORT:-8080}"
WEB_CONCURRENCY="${WEB_CONCURRENCY:-10}"
ES_USE_SSL="${ES_USE_SSL:-false}"
ES_VERIFY_CERTS="${ES_VERIFY_CERTS:-false}"
STAC_FASTAPI_RATE_LIMIT="${STAC_FASTAPI_RATE_LIMIT:-'200/minute'}"

function print_error() {
    echo -e "${RED}ERROR: $1${NC}" >&2
}

function print_warning() {
    echo -e "${YELLOW}WARNING: $1${NC}"
}

function print_success() {
    echo -e "${GREEN}SUCCESS: $1${NC}"
}

function print_info() {
    echo -e "${BLUE}INFO: $1${NC}"
}

function validate_opensearch() {
    local retry_count=0
    local max_retries=5
    local wait_time=5

    while [ $retry_count -lt $max_retries ]; do
        print_info "Checking OpenSearch connection (Attempt $((retry_count + 1))/$max_retries)..."
        
        local response_body=$(curl -k -s "http://${ES_HOST}:${ES_PORT}/" 2>/dev/null)
        local health=$(curl -k -s -o /dev/null -w '%{http_code}' "http://${ES_HOST}:${ES_PORT}/_cluster/health" 2>/dev/null)
        
        if [ "$health" -eq 200 ]; then
            if echo "$response_body" | grep -q '"distribution" *: *"opensearch"'; then
                print_success "Successfully connected to OpenSearch via HTTP"
                export ES_USE_SSL=false
                return 0
            else
                print_error "Connected to a service that is not OpenSearch"
                print_error "Found service response: $response_body"
                return 1
            fi
        fi
        
        print_info "HTTP connection failed, trying HTTPS..."
        response_body=$(curl -k -s "https://${ES_HOST}:${ES_PORT}/" 2>/dev/null)
        health=$(curl -s -o /dev/null -w '%{http_code}' "https://${ES_HOST}:${ES_PORT}/_cluster/health" 2>/dev/null)
        
        if [ "$health" -eq 200 ]; then
            if echo "$response_body" | grep -q '"distribution" *: *"opensearch"'; then
                print_success "Successfully connected to OpenSearch via HTTPS"
                export ES_USE_SSL=true
                return 0
            else
                print_error "Connected to a service that is not OpenSearch"
                print_error "Found service response: $response_body"
                return 1
            fi
        fi
        
        retry_count=$((retry_count + 1))
        if [ $retry_count -lt $max_retries ]; then
            print_warning "Connection attempt $retry_count failed. Waiting ${wait_time} seconds before retry..."
            sleep $wait_time
            wait_time=$((wait_time * 2))
        fi
    done
    
    print_error "Failed to connect to OpenSearch after $max_retries attempts:"
    print_error "  - http://${ES_HOST}:${ES_PORT}"
    print_error "  - https://${ES_HOST}:${ES_PORT}"
    print_error "Please ensure:"
    print_error "  - OpenSearch is running"
    print_error "  - ES_HOST and ES_PORT are correctly set"
    print_error "  - Network connectivity is available"
    print_error "  - SSL/TLS settings are correct if using HTTPS"
    print_error "  - You are not connecting to Elasticsearch or another service"
    return 1
}

if [ "${RUN_LOCAL_OS}" = "0" ]; then
    if [ -z "${ES_HOST}" ] || [ -z "${ES_PORT}" ]; then
        print_error "When RUN_LOCAL_OS=0, you must specify both ES_HOST and ES_PORT"
        print_error "Current settings:"
        print_error "  ES_HOST: ${ES_HOST:-not set}"
        print_error "  ES_PORT: ${ES_PORT:-not set}"
        exit 1
    fi
else
    export ES_HOST=${ES_HOST:-localhost}
    export ES_PORT=${ES_PORT:-9202}
fi

if [ "${RUN_LOCAL_OS}" = "1" ]; then
    print_info "Starting local OpenSearch instance"
    /usr/share/opensearch/bin/opensearch &
    
    print_info "Waiting for OpenSearch to be ready"
    sleep 10  # Initial wait for OpenSearch to start
    until validate_opensearch; do
        print_info "OpenSearch not yet ready. Retrying..."
        sleep 5
    done
    print_success "OpenSearch is up and running"
else
    print_info "Using external OpenSearch at ${ES_HOST}:${ES_PORT}"
    if ! validate_opensearch; then
        print_error "Cannot connect to external OpenSearch. Exiting..."
        exit 1
    fi
fi

print_info "Starting STAC FastAPI OpenSearch"
exec uvicorn stac_fastapi.opensearch.app:app \
    --host "${APP_HOST}" \
    --port "${APP_PORT}" \
    --workers "${WEB_CONCURRENCY}" \
    --reload