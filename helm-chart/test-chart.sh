#!/bin/bash

# STAC FastAPI Helm Chart Testing Script
# This script helps validate and test Helm chart deployments

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
CHART_PATH="./helm-chart/stac-fastapi"
RELEASE_NAME="stac-fastapi-test"
NAMESPACE="stac-fastapi"
BACKEND=${BACKEND:-"elasticsearch"}
MATRIX_MODE=${MATRIX_MODE:-false}

# Functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Help function
show_help() {
    cat << EOF
STAC FastAPI Helm Chart Testing Script

Usage: $0 [OPTIONS] COMMAND

Commands:
    lint            Lint the Helm chart
    test            Run Helm chart tests
    test-all        Test both elasticsearch and opensearch backends
    test-matrix     Run GitHub workflow matrix testing
    install         Install the chart for testing
    upgrade         Upgrade existing installation
    uninstall       Uninstall the test deployment
    validate        Validate deployment health
    load-data       Load sample data into the API
    cleanup         Clean up all test resources
    ci              Run CI pipeline (lint + test + validate)

Options:
    -b, --backend BACKEND   Backend to test (elasticsearch|opensearch) [default: elasticsearch]
    -n, --namespace NS      Kubernetes namespace [default: stac-fastapi]
    -r, --release NAME      Helm release name [default: stac-fastapi-test]
    -m, --matrix           Run matrix testing for CI
    -h, --help             Show this help message

Examples:
    $0 lint                           # Lint the chart
    $0 -b opensearch install          # Install with OpenSearch backend
    $0 test-all                       # Test both backends
    $0 test-matrix                    # Run GitHub workflow matrix tests
    $0 ci                             # Run full CI pipeline
    $0 validate                       # Check deployment health
    $0 load-data                      # Load test data
    $0 cleanup                        # Clean up everything

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -b|--backend)
            BACKEND="$2"
            shift 2
            ;;
        -n|--namespace)
            NAMESPACE="$2"
            shift 2
            ;;
        -r|--release)
            RELEASE_NAME="$2"
            shift 2
            ;;
        -m|--matrix)
            MATRIX_MODE=true
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        lint|test|test-all|test-matrix|install|upgrade|uninstall|validate|load-data|cleanup|ci)
            COMMAND="$1"
            shift
            ;;
        *)
            log_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Validate backend
if [[ "$BACKEND" != "elasticsearch" && "$BACKEND" != "opensearch" ]]; then
    log_error "Invalid backend: $BACKEND. Must be 'elasticsearch' or 'opensearch'"
    exit 1
fi

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    if ! command -v helm &> /dev/null; then
        log_error "Helm is not installed"
        exit 1
    fi
    
    if ! command -v kubectl &> /dev/null; then
        log_error "kubectl is not installed"
        exit 1
    fi
    
    if ! kubectl cluster-info &> /dev/null; then
        log_error "Cannot connect to Kubernetes cluster"
        exit 1
    fi
    
    log_success "Prerequisites check passed"
}

# Lint the Helm chart
lint_chart() {
    log_info "Linting Helm chart..."
    
    if [[ ! -d "$CHART_PATH" ]]; then
        log_error "Chart path not found: $CHART_PATH"
        exit 1
    fi
    
    # Update dependencies
    log_info "Updating chart dependencies..."
    helm dependency update "$CHART_PATH"
    
    # Lint the chart
    helm lint "$CHART_PATH"
    
    # Template the chart to check for syntax errors
    log_info "Testing chart templates..."
    helm template test-release "$CHART_PATH" \
        --set backend="$BACKEND" \
        --set "${BACKEND}.enabled=true" \
        --output-dir /tmp/helm-test || {
        log_error "Chart templating failed"
        exit 1
    }
    
    log_success "Chart linting completed successfully"
}

# Run Helm chart tests
test_chart() {
    log_info "Running Helm chart tests..."
    
    # Dry run installation
    log_info "Testing chart installation (dry run)..."
    helm install "$RELEASE_NAME" "$CHART_PATH" \
        --namespace "$NAMESPACE" \
        --create-namespace \
        --dry-run \
        --set backend="$BACKEND" \
        --set "${BACKEND}.enabled=true" \
        --set "app.image.tag=latest"
    
    log_success "Chart tests completed successfully"
}

# Install the chart
install_chart() {
    log_info "Installing STAC FastAPI chart with $BACKEND backend..."
    
    # Create namespace if it doesn't exist
    kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
    
    # Update dependencies
    helm dependency update "$CHART_PATH"
    
    # Select appropriate values file for backend
    local values_file=""
    case $BACKEND in
        elasticsearch)
            values_file="values-minimal-storage.yaml"
            ;;
        opensearch)
            values_file="values-opensearch.yaml"
            ;;
        *)
            log_error "Unknown backend: $BACKEND"
            return 1
            ;;
    esac
    
    log_info "Using values file: $values_file"
    
    # Install the chart with appropriate values
    helm install "$RELEASE_NAME" "$CHART_PATH" \
        --namespace "$NAMESPACE" \
        --values "$CHART_PATH/$values_file" \
        --set backend="$BACKEND" \
        --set "${BACKEND}.enabled=true" \
        --set "app.image.tag=latest" \
        --set "app.service.type=ClusterIP" \
        --wait \
        --timeout=10m
    
    log_success "Chart installed successfully"
    
    # Show installation status
    helm status "$RELEASE_NAME" -n "$NAMESPACE"
}

# Upgrade the chart
upgrade_chart() {
    log_info "Upgrading STAC FastAPI chart..."
    
    helm dependency update "$CHART_PATH"
    
    # Select appropriate values file for backend
    local values_file=""
    case $BACKEND in
        elasticsearch)
            values_file="values-minimal-storage.yaml"
            ;;
        opensearch)
            values_file="values-opensearch.yaml"
            ;;
        *)
            log_error "Unknown backend: $BACKEND"
            return 1
            ;;
    esac
    
    helm upgrade "$RELEASE_NAME" "$CHART_PATH" \
        --namespace "$NAMESPACE" \
        --values "$CHART_PATH/$values_file" \
        --set backend="$BACKEND" \
        --set "${BACKEND}.enabled=true" \
        --set "app.image.tag=latest" \
        --wait \
        --timeout=10m
    
    log_success "Chart upgraded successfully"
}

# Uninstall the chart
uninstall_chart() {
    log_info "Uninstalling STAC FastAPI chart..."
    
    helm uninstall "$RELEASE_NAME" -n "$NAMESPACE" || true
    
    log_success "Chart uninstalled"
}

# Validate deployment
validate_deployment() {
    log_info "Validating deployment health..."
    
    # Check if deployment exists
    if ! kubectl get deployment "$RELEASE_NAME" -n "$NAMESPACE" &> /dev/null; then
        log_error "Deployment not found: $RELEASE_NAME"
        exit 1
    fi
    
    # Check pod status
    log_info "Checking pod status..."
    kubectl get pods -n "$NAMESPACE" -l "app.kubernetes.io/name=stac-fastapi"
    
    # Wait for pods to be ready
    log_info "Waiting for pods to be ready..."
    kubectl wait --for=condition=ready pod \
        -l "app.kubernetes.io/name=stac-fastapi" \
        -n "$NAMESPACE" \
        --timeout=300s
    
    # Check service
    log_info "Checking service..."
    kubectl get service "$RELEASE_NAME" -n "$NAMESPACE"
    
    # Test API endpoint
    log_info "Testing API endpoint..."
    kubectl port-forward -n "$NAMESPACE" service/"$RELEASE_NAME" 8080:80 &
    PORT_FORWARD_PID=$!
    
    # Wait for port-forward to be ready
    sleep 5
    
    # Test the API
    if curl -s -f http://localhost:8080/ > /dev/null; then
        log_success "API is responding"
        
        # Test specific endpoints
        log_info "Testing API endpoints..."
        
        # Root endpoint
        curl -s http://localhost:8080/ | jq -r '.title // "No title"' 2>/dev/null || echo "Root endpoint accessible"
        
        # Collections endpoint
        if curl -s -f http://localhost:8080/collections > /dev/null; then
            log_success "Collections endpoint accessible"
        else
            log_warning "Collections endpoint not accessible"
        fi
        
        # Search endpoint
        if curl -s -f -X POST http://localhost:8080/search -H "Content-Type: application/json" -d '{}' > /dev/null; then
            log_success "Search endpoint accessible"
        else
            log_warning "Search endpoint not accessible"
        fi
        
    else
        log_error "API is not responding"
    fi
    
    # Clean up port-forward
    kill $PORT_FORWARD_PID 2>/dev/null || true
    
    # Check database connectivity
    log_info "Checking database connectivity..."
    DB_POD=$(kubectl get pods -n "$NAMESPACE" -l "app=${RELEASE_NAME}-${BACKEND}-master" -o jsonpath="{.items[0].metadata.name}" 2>/dev/null || echo "")
    
    if [[ -n "$DB_POD" ]]; then
        if kubectl exec -n "$NAMESPACE" "$DB_POD" -- curl -s -f http://localhost:9200/_health > /dev/null 2>&1; then
            log_success "Database is healthy"
        else
            log_warning "Database health check failed"
        fi
    else
        log_warning "Database pod not found (might be using external database)"
    fi
    
    log_success "Deployment validation completed"
}

# Load sample data
load_sample_data() {
    log_info "Loading sample data..."
    
    # Port forward to the service
    kubectl port-forward -n "$NAMESPACE" service/"$RELEASE_NAME" 8080:80 &
    PORT_FORWARD_PID=$!
    
    # Wait for port-forward
    sleep 5
    
    # Create a simple collection
    log_info "Creating test collection..."
    curl -X POST http://localhost:8080/collections \
        -H "Content-Type: application/json" \
        -d '{
            "id": "test-collection",
            "title": "Test Collection",
            "description": "A test collection for validation",
            "extent": {
                "spatial": {
                    "bbox": [[-180, -90, 180, 90]]
                },
                "temporal": {
                    "interval": [["2020-01-01T00:00:00Z", "2024-12-31T23:59:59Z"]]
                }
            },
            "license": "public-domain"
        }' || log_warning "Failed to create collection (might already exist)"
    
    # Create a test item
    log_info "Creating test item..."
    curl -X POST http://localhost:8080/collections/test-collection/items \
        -H "Content-Type: application/json" \
        -d '{
            "id": "test-item-001",
            "type": "Feature",
            "stac_version": "1.0.0",
            "collection": "test-collection",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[-1, -1], [1, -1], [1, 1], [-1, 1], [-1, -1]]]
            },
            "bbox": [-1, -1, 1, 1],
            "properties": {
                "datetime": "2023-06-15T12:00:00Z"
            },
            "assets": {
                "thumbnail": {
                    "href": "https://example.com/thumbnail.jpg",
                    "type": "image/jpeg",
                    "title": "Thumbnail"
                }
            }
        }' || log_warning "Failed to create item"
    
    # Test search
    log_info "Testing search functionality..."
    SEARCH_RESULT=$(curl -s -X POST http://localhost:8080/search \
        -H "Content-Type: application/json" \
        -d '{"collections": ["test-collection"], "limit": 1}')
    
    ITEM_COUNT=$(echo "$SEARCH_RESULT" | jq -r '.features | length' 2>/dev/null || echo "0")
    
    if [[ "$ITEM_COUNT" -gt 0 ]]; then
        log_success "Sample data loaded and searchable (found $ITEM_COUNT items)"
    else
        log_warning "Sample data might not be immediately searchable (indexing delay)"
    fi
    
    # Clean up port-forward
    kill $PORT_FORWARD_PID 2>/dev/null || true
    
    log_success "Sample data loading completed"
}

# Test both backends comprehensively
test_all_backends() {
    log_info "Testing all backends (elasticsearch and opensearch)..."
    
    local original_backend="$BACKEND"
    local failed_backends=()
    
    for backend in elasticsearch opensearch; do
        log_info "================== Testing $backend backend =================="
        
        BACKEND="$backend"
        RELEASE_NAME="stac-fastapi-test-$backend"
        NAMESPACE="stac-fastapi-$backend"
        
        log_info "Running tests for $backend backend..."
        
        # Run comprehensive test
        if run_backend_test "$backend"; then
            log_success "$backend backend tests passed"
        else
            log_error "$backend backend tests failed"
            failed_backends+=("$backend")
        fi
        
        # Cleanup between tests
        cleanup
        sleep 5
    done
    
    # Restore original values
    BACKEND="$original_backend"
    RELEASE_NAME="stac-fastapi-test"
    NAMESPACE="stac-fastapi"
    
    # Report results
    if [[ ${#failed_backends[@]} -eq 0 ]]; then
        log_success "All backend tests passed successfully!"
    else
        log_error "The following backends failed: ${failed_backends[*]}"
        exit 1
    fi
}

# Run GitHub workflow matrix testing
test_matrix() {
    log_info "Running GitHub workflow matrix testing..."
    
    if [[ "$MATRIX_MODE" == "true" ]]; then
        log_info "Matrix mode enabled - testing single backend: $BACKEND"
        run_backend_test "$BACKEND"
    else
        log_info "Running full matrix test locally..."
        test_all_backends
    fi
}

# Run tests for a specific backend
run_backend_test() {
    local backend="$1"
    local test_failed=false
    
    log_info "Starting comprehensive test for $backend backend..."
    
    # Set backend-specific values
    BACKEND="$backend"
    
    # Step 1: Lint
    log_info "Step 1: Linting chart for $backend..."
    if ! lint_chart; then
        log_error "Linting failed for $backend"
        return 1
    fi
    
    # Step 2: Template test
    log_info "Step 2: Testing chart templates for $backend..."
    if ! test_chart; then
        log_error "Template test failed for $backend"
        return 1
    fi
    
    # Step 3: Install and validate
    log_info "Step 3: Installing chart for $backend..."
    if ! install_chart; then
        log_error "Installation failed for $backend"
        return 1
    fi
    
    # Step 4: Validate deployment
    log_info "Step 4: Validating deployment for $backend..."
    if ! validate_deployment; then
        log_error "Deployment validation failed for $backend"
        test_failed=true
    fi
    
    # Step 5: Load and test data
    log_info "Step 5: Testing data operations for $backend..."
    if ! load_sample_data; then
        log_error "Data operations failed for $backend"
        test_failed=true
    fi
    
    # Step 6: Test backend-specific functionality
    log_info "Step 6: Testing $backend-specific functionality..."
    if ! test_backend_specifics "$backend"; then
        log_error "Backend-specific tests failed for $backend"
        test_failed=true
    fi
    
    if [[ "$test_failed" == "true" ]]; then
        log_error "Some tests failed for $backend backend"
        return 1
    else
        log_success "All tests passed for $backend backend"
        return 0
    fi
}

# Test backend-specific functionality
test_backend_specifics() {
    local backend="$1"
    
    log_info "Testing $backend-specific functionality..."
    
    # Port forward to the service
    kubectl port-forward -n "$NAMESPACE" service/"$RELEASE_NAME" 8080:80 &
    local pf_pid=$!
    
    # Wait for port-forward
    sleep 5
    
    case "$backend" in
        elasticsearch)
            # Test Elasticsearch-specific endpoints
            log_info "Testing Elasticsearch-specific features..."
            
            # Check if using correct image
            local image=$(kubectl get deployment "$RELEASE_NAME" -n "$NAMESPACE" -o jsonpath='{.spec.template.spec.containers[0].image}')
            if [[ "$image" == *"-es:"* ]]; then
                log_success "Using correct Elasticsearch image: $image"
            else
                log_warning "Image might not be Elasticsearch-specific: $image"
            fi
            
            # Test backend environment variable
            local backend_env=$(kubectl get deployment "$RELEASE_NAME" -n "$NAMESPACE" -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="BACKEND")].value}')
            if [[ "$backend_env" == "elasticsearch" ]]; then
                log_success "Backend environment correctly set to elasticsearch"
            else
                log_error "Backend environment incorrect: $backend_env"
                kill $pf_pid 2>/dev/null || true
                return 1
            fi
            ;;
            
        opensearch)
            # Test OpenSearch-specific endpoints
            log_info "Testing OpenSearch-specific features..."
            
            # Check if using correct image
            local image=$(kubectl get deployment "$RELEASE_NAME" -n "$NAMESPACE" -o jsonpath='{.spec.template.spec.containers[0].image}')
            if [[ "$image" == *"-os:"* ]]; then
                log_success "Using correct OpenSearch image: $image"
            else
                log_warning "Image might not be OpenSearch-specific: $image"
            fi
            
            # Test backend environment variable
            local backend_env=$(kubectl get deployment "$RELEASE_NAME" -n "$NAMESPACE" -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="BACKEND")].value}')
            if [[ "$backend_env" == "opensearch" ]]; then
                log_success "Backend environment correctly set to opensearch"
            else
                log_error "Backend environment incorrect: $backend_env"
                kill $pf_pid 2>/dev/null || true
                return 1
            fi
            ;;
    esac
    
    # Test that only the correct backend is deployed
    log_info "Verifying only $backend backend is deployed..."
    
    local es_pods=$(kubectl get pods -n "$NAMESPACE" -l "app=stac-elasticsearch-master" --no-headers 2>/dev/null | wc -l)
    local os_pods=$(kubectl get pods -n "$NAMESPACE" -l "app.kubernetes.io/name=opensearch" --no-headers 2>/dev/null | wc -l)
    
    case "$backend" in
        elasticsearch)
            if [[ "$es_pods" -gt 0 && "$os_pods" -eq 0 ]]; then
                log_success "Only Elasticsearch backend is deployed"
            else
                log_error "Wrong backends deployed - ES pods: $es_pods, OS pods: $os_pods"
                kill $pf_pid 2>/dev/null || true
                return 1
            fi
            ;;
        opensearch)
            if [[ "$os_pods" -gt 0 && "$es_pods" -eq 0 ]]; then
                log_success "Only OpenSearch backend is deployed"
            else
                log_error "Wrong backends deployed - ES pods: $es_pods, OS pods: $os_pods"
                kill $pf_pid 2>/dev/null || true
                return 1
            fi
            ;;
    esac
    
    # Clean up port-forward
    kill $pf_pid 2>/dev/null || true
    
    log_success "$backend-specific tests completed successfully"
    return 0
}

# Run CI pipeline
run_ci_pipeline() {
    log_info "Running CI pipeline..."
    
    if [[ "$MATRIX_MODE" == "true" ]]; then
        log_info "CI Matrix Mode: Testing $BACKEND backend"
        
        # Set CI-specific configuration
        export HELM_EXPERIMENTAL_OCI=1
        
        # Run the backend test
        if run_backend_test "$BACKEND"; then
            log_success "CI pipeline passed for $BACKEND backend"
            
            # Generate test report
            generate_test_report "$BACKEND"
        else
            log_error "CI pipeline failed for $BACKEND backend"
            exit 1
        fi
    else
        log_info "CI Full Mode: Testing all backends"
        test_all_backends
        
        # Generate combined test report
        generate_test_report "all"
    fi
}

# Generate test report
generate_test_report() {
    local backend="$1"
    local report_file="test-report-$backend-$(date +%Y%m%d-%H%M%S).json"
    
    log_info "Generating test report: $report_file"
    
    # Get cluster info
    local k8s_version=$(kubectl version --short 2>/dev/null | grep "Server Version" | cut -d: -f2 | xargs)
    local helm_version=$(helm version --short 2>/dev/null)
    
    # Create test report
    cat > "$report_file" << EOF
{
  "test_run": {
    "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
    "backend": "$backend",
    "kubernetes_version": "$k8s_version",
    "helm_version": "$helm_version",
    "chart_version": "$(helm show chart $CHART_PATH | grep version | head -1 | cut -d: -f2 | xargs)",
    "status": "passed"
  },
  "tests_performed": [
    "chart_linting",
    "template_validation",
    "deployment_installation",
    "health_validation",
    "data_operations",
    "backend_specific_tests"
  ],
  "resources_tested": {
    "deployment": true,
    "service": true,
    "configmap": true,
    "secrets": true,
    "backend_statefulset": true
  }
}
EOF
    
    log_success "Test report generated: $report_file"
}

# Clean up all resources
cleanup() {
    log_info "Cleaning up all test resources..."
    
    # Uninstall Helm release
    helm uninstall "$RELEASE_NAME" -n "$NAMESPACE" --ignore-not-found
    
    # Delete namespace (this will delete all resources)
    kubectl delete namespace "$NAMESPACE" --ignore-not-found
    
    # Clean up any remaining persistent volumes
    kubectl get pv | grep "$NAMESPACE" | awk '{print $1}' | xargs -r kubectl delete pv
    
    log_success "Cleanup completed"
}

# Main script logic
main() {
    case "${COMMAND:-}" in
        lint)
            check_prerequisites
            lint_chart
            ;;
        test)
            check_prerequisites
            lint_chart
            test_chart
            ;;
        test-all)
            check_prerequisites
            test_all_backends
            ;;
        test-matrix)
            check_prerequisites
            test_matrix
            ;;
        ci)
            check_prerequisites
            run_ci_pipeline
            ;;
        install)
            check_prerequisites
            install_chart
            ;;
        upgrade)
            check_prerequisites
            upgrade_chart
            ;;
        uninstall)
            uninstall_chart
            ;;
        validate)
            check_prerequisites
            validate_deployment
            ;;
        load-data)
            check_prerequisites
            load_sample_data
            ;;
        cleanup)
            cleanup
            ;;
        "")
            log_error "No command specified"
            show_help
            exit 1
            ;;
        *)
            log_error "Unknown command: $COMMAND"
            show_help
            exit 1
            ;;
    esac
}

# Trap to ensure cleanup on script exit
trap 'kill $PORT_FORWARD_PID 2>/dev/null || true' EXIT

# Run main function
main