#!/bin/bash
set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

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

function validate_elasticsearch() {
    local retry_count=0
    local max_retries=5
    local wait_time=5

    while [ $retry_count -lt $max_retries ]; do
        print_info "Checking Elasticsearch connection (Attempt $((retry_count + 1))/$max_retries)..."
        
        health=$(curl -k -s -o /dev/null -w '%{http_code}' "http://${ES_HOST}:${ES_PORT}/_cluster/health" 2>/dev/null)
        
        if [ "$health" -eq 200 ]; then
            print_success "Successfully connected to Elasticsearch via HTTP"
            export ES_USE_SSL=false
            return 0
        fi
        
        print_info "HTTP connection failed, trying HTTPS..."
        health=$(curl -s -o /dev/null -w '%{http_code}' "https://${ES_HOST}:${ES_PORT}/_cluster/health" 2>/dev/null)
        
        if [ "$health" -eq 200 ]; then
            print_success "Successfully connected to Elasticsearch via HTTPS"
            export ES_USE_SSL=true
            return 0
        fi
        
        retry_count=$((retry_count + 1))
        if [ $retry_count -lt $max_retries ]; then
            print_warning "Connection attempt $retry_count failed. Waiting ${wait_time} seconds before retry..."
            sleep $wait_time
            wait_time=$((wait_time * 2))
        fi
    done
    
    print_error "Failed to connect to Elasticsearch after $max_retries attempts:"
    print_error "  - http://${ES_HOST}:${ES_PORT}"
    print_error "  - https://${ES_HOST}:${ES_PORT}"
    print_error "Please ensure:"
    print_error "  - Elasticsearch is running"
    print_error "  - ES_HOST and ES_PORT are correctly set"
    print_error "  - Network connectivity is available"
    print_error "  - SSL/TLS settings are correct if using HTTPS"
    return 1
}

if [ "${RUN_LOCAL_ES}" = "0" ]; then
    if [ -z "${ES_HOST}" ] || [ -z "${ES_PORT}" ]; then
        print_error "When RUN_LOCAL_ES=0, you must specify both ES_HOST and ES_PORT"
        print_error "Current settings:"
        print_error "  ES_HOST: ${ES_HOST:-not set}"
        print_error "  ES_PORT: ${ES_PORT:-not set}"
        exit 1
    fi
else
    export ES_HOST=${ES_HOST:-localhost}
    export ES_PORT=${ES_PORT:-9200}
fi

if [ "${RUN_LOCAL_ES}" = "1" ]; then
    print_info "Starting local Elasticsearch instance"
    /usr/share/elasticsearch/bin/elasticsearch &
    
    print_info "Waiting for Elasticsearch to be ready"
    until validate_elasticsearch; do
        print_info "Elasticsearch not yet ready. Retrying..."
        sleep 5
    done
    print_success "Elasticsearch is up and running"
else
    print_info "Using external Elasticsearch at ${ES_HOST}:${ES_PORT}"
    if ! validate_elasticsearch; then
        print_error "Cannot connect to external Elasticsearch. Exiting..."
        exit 1
    fi
fi

print_info "Starting STAC FastAPI Elasticsearch"
exec uvicorn stac_fastapi.elasticsearch.app:app \
    --host "${APP_HOST}" \
    --port "${APP_PORT}" \
    --workers "${WEB_CONCURRENCY}" \
    --reload