# STAC FastAPI Kubernetes Deployment Guide

This guide provides comprehensive instructions for deploying STAC FastAPI on Kubernetes using the provided Helm chart.

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Quick Start](#quick-start)
4. [Deployment Options](#deployment-options)
5. [Configuration](#configuration)
6. [Production Deployment](#production-deployment)
7. [Monitoring and Observability](#monitoring-and-observability)
8. [Troubleshooting](#troubleshooting)
9. [Maintenance](#maintenance)

## Overview

The STAC FastAPI Helm chart supports multiple deployment configurations:

- **Elasticsearch Backend**: Deploy with bundled or external Elasticsearch
- **OpenSearch Backend**: Deploy with bundled or external OpenSearch
- **High Availability**: Multi-replica deployments with proper load balancing
- **Auto-scaling**: Horizontal Pod Autoscaler based on CPU/memory metrics
- **Security**: Network policies, RBAC, and pod security contexts
- **Monitoring**: Prometheus metrics and Grafana dashboards

## Prerequisites

### Required

- Kubernetes cluster (v1.16+)
- Helm 3.0+
- kubectl configured to access your cluster
- Sufficient cluster resources (see resource requirements below)

### Optional

- NGINX Ingress Controller (for ingress)
- cert-manager (for TLS certificates)
- Prometheus Operator (for monitoring)
- Persistent storage class (for database persistence)

### Resource Requirements

**Minimum (Development)**:
- 2 CPU cores
- 4 GB RAM
- 20 GB storage

**Recommended (Production)**:
- 8 CPU cores
- 16 GB RAM
- 200 GB SSD storage

## Quick Start

### 1. Clone and Prepare

```bash
git clone https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch.git
cd stac-fastapi-elasticsearch-opensearch/helm-chart
```

### 2. Update Dependencies

```bash
helm dependency update stac-fastapi
```

### 3. Install with Elasticsearch

```bash
helm install my-stac-api stac-fastapi \
  --set backend=elasticsearch \
  --set elasticsearch.enabled=true \
  --set opensearch.enabled=false \
  --create-namespace \
  --namespace stac-fastapi
```

### 4. Install with OpenSearch

```bash
helm install my-stac-api stac-fastapi \
  --set backend=opensearch \
  --set elasticsearch.enabled=false \
  --set opensearch.enabled=true \
  --create-namespace \
  --namespace stac-fastapi
```

### 5. Access the API

```bash
# Port forward to access locally
kubectl port-forward -n stac-fastapi service/my-stac-api 8080:80

# Test the API
curl http://localhost:8080/
curl http://localhost:8080/collections
```

## Deployment Options

### 1. Development Deployment

**Use case**: Local development, testing, minimal resources

```bash
helm install dev-stac stac-fastapi \
  --set backend=elasticsearch \
  --set app.replicaCount=1 \
  --set elasticsearch.replicas=1 \
  --set elasticsearch.minimumMasterNodes=1 \
  --set elasticsearch.resources.requests.memory=1Gi \
  --set elasticsearch.volumeClaimTemplate.resources.requests.storage=10Gi
```

### 2. Production Deployment with Elasticsearch

**Use case**: High-availability production environment

```bash
helm install prod-stac stac-fastapi \
  --values stac-fastapi/values-elasticsearch.yaml \
  --set app.ingress.enabled=true \
  --set app.ingress.hosts[0].host=stac-api.yourdomain.com \
  --set elasticsearch.volumeClaimTemplate.storageClassName=fast-ssd
```

### 3. Production Deployment with OpenSearch

**Use case**: Production environment with OpenSearch preference

```bash
helm install prod-stac stac-fastapi \
  --values stac-fastapi/values-opensearch.yaml \
  --set app.ingress.enabled=true \
  --set app.ingress.hosts[0].host=stac-api.yourdomain.com \
  --set opensearch.persistence.storageClass=fast-ssd
```

### 4. External Database Deployment

**Use case**: Connect to existing Elasticsearch/OpenSearch cluster

```bash
# First create secret for API key
kubectl create secret generic elasticsearch-credentials \
  --from-literal=api-key="your-api-key-here"

# Deploy
helm install external-stac stac-fastapi \
  --values stac-fastapi/values-external.yaml \
  --set externalDatabase.host=elasticsearch.example.com \
  --set externalDatabase.port=9200 \
  --set externalDatabase.ssl=true
```

### 5. Multi-Environment Deployment

**Use case**: Deploy multiple environments (dev, staging, prod)

```bash
# Development
helm install dev-stac stac-fastapi \
  --namespace stac-dev \
  --create-namespace \
  --set backend=elasticsearch \
  --set app.env.ENVIRONMENT=development

# Staging
helm install staging-stac stac-fastapi \
  --namespace stac-staging \
  --create-namespace \
  --values stac-fastapi/values-elasticsearch.yaml \
  --set app.env.ENVIRONMENT=staging

# Production
helm install prod-stac stac-fastapi \
  --namespace stac-production \
  --create-namespace \
  --values stac-fastapi/values-elasticsearch.yaml \
  --set app.env.ENVIRONMENT=production
```

## Configuration

### Application Configuration

```yaml
app:
  env:
    # API Configuration
    STAC_FASTAPI_TITLE: "My STAC API"
    STAC_FASTAPI_DESCRIPTION: "A production STAC API"
    STAC_FASTAPI_VERSION: "6.0.0"
    
    # Performance Tuning
    WEB_CONCURRENCY: "8"                    # Number of worker processes
    ENABLE_DIRECT_RESPONSE: "true"          # Maximum performance mode
    DATABASE_REFRESH: "false"               # Better bulk performance
    
    # Large Dataset Optimization
    ENABLE_DATETIME_INDEX_FILTERING: "true" # Temporal partitioning
    DATETIME_INDEX_MAX_SIZE_GB: "50"        # Index size limit
    
    # Rate Limiting
    STAC_FASTAPI_RATE_LIMIT: "1000/minute" # API rate limit
    
    # Feature Toggles
    ENABLE_TRANSACTIONS_EXTENSIONS: "true"  # Enable POST operations
    STAC_INDEX_ASSETS: "true"              # Index asset metadata
```

### Database Configuration

#### Elasticsearch Production Settings

```yaml
elasticsearch:
  replicas: 3
  minimumMasterNodes: 2
  
  esConfig:
    elasticsearch.yml: |
      # Cluster settings
      cluster.name: "stac-elasticsearch-prod"
      action.destructive_requires_name: true
      
      # Performance tuning
      indices.memory.index_buffer_size: 20%
      thread_pool.write.queue_size: 1000
      thread_pool.search.queue_size: 1000
      
      # Disk usage thresholds
      cluster.routing.allocation.disk.threshold_enabled: true
      cluster.routing.allocation.disk.watermark.low: 85%
      cluster.routing.allocation.disk.watermark.high: 90%
      cluster.routing.allocation.disk.watermark.flood_stage: 95%
  
  resources:
    requests:
      cpu: "2000m"
      memory: "8Gi"
    limits:
      cpu: "4000m"
      memory: "8Gi"
  
  esJavaOpts: "-Xmx4g -Xms4g"
```

#### OpenSearch Production Settings

```yaml
opensearch:
  replicas: 3
  
  config:
    opensearch.yml: |
      # Cluster settings
      cluster.name: stac-opensearch-prod
      action.destructive_requires_name: true
      
      # Performance tuning
      indices.memory.index_buffer_size: 20%
      thread_pool.write.queue_size: 1000
      thread_pool.search.queue_size: 1000
      
      # Disk usage thresholds
      cluster.routing.allocation.disk.threshold_enabled: true
      cluster.routing.allocation.disk.watermark.low: 85%
      cluster.routing.allocation.disk.watermark.high: 90%
      cluster.routing.allocation.disk.watermark.flood_stage: 95%
  
  resources:
    requests:
      cpu: "2000m"
      memory: "8Gi"
    limits:
      cpu: "4000m"
      memory: "8Gi"
  
  opensearchJavaOpts: "-Xmx4g -Xms4g"
```

### Ingress Configuration

```yaml
app:
  ingress:
    enabled: true
    className: "nginx"
    annotations:
      nginx.ingress.kubernetes.io/rewrite-target: /
      nginx.ingress.kubernetes.io/proxy-body-size: "100m"
      nginx.ingress.kubernetes.io/proxy-read-timeout: "300"
      cert-manager.io/cluster-issuer: "letsencrypt-prod"
      
    hosts:
      - host: stac-api.yourdomain.com
        paths:
          - path: /
            pathType: Prefix
            
    tls:
      - secretName: stac-api-tls
        hosts:
          - stac-api.yourdomain.com
```

## Production Deployment

### 1. Pre-deployment Checklist

- [ ] Kubernetes cluster properly sized
- [ ] Storage classes configured
- [ ] Ingress controller installed
- [ ] DNS records configured
- [ ] SSL certificates ready
- [ ] Monitoring stack deployed
- [ ] Backup strategy defined

### 2. Production Values File

Create a production-specific values file:

```yaml
# values-production.yaml
backend: elasticsearch

app:
  replicaCount: 3
  
  image:
    tag: "v6.0.0"  # Use specific version
    pullPolicy: IfNotPresent
  
  resources:
    requests:
      cpu: "1000m"
      memory: "2Gi"
    limits:
      cpu: "2000m"
      memory: "4Gi"
  
  autoscaling:
    enabled: true
    minReplicas: 3
    maxReplicas: 20
    targetCPUUtilizationPercentage: 70
    targetMemoryUtilizationPercentage: 80
  
  env:
    ENVIRONMENT: "production"
    WEB_CONCURRENCY: "8"
    ENABLE_DIRECT_RESPONSE: "true"
    DATABASE_REFRESH: "false"
    ENABLE_DATETIME_INDEX_FILTERING: "true"
    DATETIME_INDEX_MAX_SIZE_GB: "100"
    STAC_FASTAPI_RATE_LIMIT: "2000/minute"

elasticsearch:
  enabled: true
  replicas: 5
  minimumMasterNodes: 3
  
  resources:
    requests:
      cpu: "2000m"
      memory: "8Gi"
    limits:
      cpu: "4000m"
      memory: "8Gi"
  
  volumeClaimTemplate:
    storageClassName: "fast-ssd"
    resources:
      requests:
        storage: 500Gi

podDisruptionBudget:
  enabled: true
  minAvailable: 2

monitoring:
  enabled: true
  prometheus:
    enabled: true
    serviceMonitor:
      enabled: true

networkPolicy:
  enabled: true
```

### 3. Deploy to Production

```bash
# Deploy
helm install stac-prod stac-fastapi \
  --namespace stac-production \
  --create-namespace \
  --values values-production.yaml \
  --wait \
  --timeout=15m

# Verify deployment
kubectl get all -n stac-production
helm status stac-prod -n stac-production
```

### 4. Post-deployment Verification

```bash
# Test API endpoints
curl https://stac-api.yourdomain.com/
curl https://stac-api.yourdomain.com/collections
curl -X POST https://stac-api.yourdomain.com/search \
  -H "Content-Type: application/json" \
  -d '{}'

# Check database health
kubectl port-forward -n stac-production \
  service/stac-prod-elasticsearch-master 9200:9200 &
curl http://localhost:9200/_cluster/health
```

## Monitoring and Observability

### 1. Enable Prometheus Monitoring

```yaml
monitoring:
  enabled: true
  prometheus:
    enabled: true
    serviceMonitor:
      enabled: true
      interval: 30s
      scrapeTimeout: 10s
```

### 2. Grafana Dashboards

Create custom dashboards for:
- API request rates and latency
- Database performance metrics
- Resource utilization
- Error rates and patterns

### 3. Alerting Rules

Example Prometheus alerting rules:

```yaml
# stac-fastapi-alerts.yaml
groups:
  - name: stac-fastapi
    rules:
      - alert: STACAPIHighErrorRate
        expr: rate(http_requests_total{status=~"5.."}[5m]) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High error rate detected"
          
      - alert: STACAPIHighLatency
        expr: histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m])) > 2
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High latency detected"
```

### 4. Log Aggregation

Configure log shipping to your preferred logging solution:

```yaml
app:
  podAnnotations:
    fluentd.io/include: "true"
    fluentd.io/exclude: "false"
```

## Troubleshooting

### Common Issues

#### 1. Pod Startup Issues

```bash
# Check pod status
kubectl get pods -n stac-fastapi

# View pod logs
kubectl logs -f deployment/my-stac-api -n stac-fastapi

# Describe pod for events
kubectl describe pod <pod-name> -n stac-fastapi
```

#### 2. Database Connectivity Issues

```bash
# Check database service
kubectl get svc -n stac-fastapi

# Test database connectivity
kubectl exec -it deployment/my-stac-api -n stac-fastapi -- \
  curl http://elasticsearch:9200/_health

# Check database logs
kubectl logs -f statefulset/my-stac-api-elasticsearch-master -n stac-fastapi
```

#### 3. Performance Issues

```bash
# Check resource usage
kubectl top pods -n stac-fastapi
kubectl top nodes

# Check HPA status
kubectl get hpa -n stac-fastapi

# View detailed metrics
kubectl describe hpa my-stac-api -n stac-fastapi
```

#### 4. Ingress Issues

```bash
# Check ingress status
kubectl get ingress -n stac-fastapi

# Check ingress controller logs
kubectl logs -f deployment/nginx-controller -n nginx-ingress

# Test DNS resolution
nslookup stac-api.yourdomain.com
```

### Debugging Tools

Use the provided test script for comprehensive debugging:

```bash
# Validate deployment
./test-chart.sh validate

# Load test data
./test-chart.sh load-data

# Full cleanup
./test-chart.sh cleanup
```

## Maintenance

### 1. Upgrades

```bash
# Update chart dependencies
helm dependency update stac-fastapi

# Upgrade deployment
helm upgrade stac-prod stac-fastapi \
  --namespace stac-production \
  --values values-production.yaml \
  --wait

# Rollback if needed
helm rollback stac-prod 1 -n stac-production
```

### 2. Scaling

```bash
# Manual scaling
kubectl scale deployment stac-prod \
  --replicas=5 -n stac-production

# Update HPA limits
helm upgrade stac-prod stac-fastapi \
  --set app.autoscaling.maxReplicas=30 \
  --reuse-values
```

### 3. Backup and Restore

#### Database Snapshots

```bash
# Create snapshot
kubectl exec -it stac-prod-elasticsearch-master-0 -n stac-production -- \
  curl -X PUT "localhost:9200/_snapshot/backup/snapshot_$(date +%Y%m%d)" \
  -H 'Content-Type: application/json' \
  -d '{"indices": "*", "ignore_unavailable": true}'

# List snapshots
kubectl exec -it stac-prod-elasticsearch-master-0 -n stac-production -- \
  curl "localhost:9200/_snapshot/backup/_all"
```

#### Configuration Backup

```bash
# Backup Helm values
helm get values stac-prod -n stac-production > backup-values.yaml

# Backup Kubernetes resources
kubectl get all -n stac-production -o yaml > backup-resources.yaml
```

### 4. Security Updates

```bash
# Update to latest images
helm upgrade stac-prod stac-fastapi \
  --set app.image.tag=latest \
  --set elasticsearch.imageTag=8.11.0 \
  --reuse-values

# Apply security patches
kubectl patch deployment stac-prod \
  -n stac-production \
  -p '{"spec":{"template":{"metadata":{"annotations":{"kubectl.kubernetes.io/restartedAt":"'$(date +%Y-%m-%dT%H:%M:%S%z)'"}}}}}'
```

This deployment guide provides comprehensive instructions for deploying and maintaining STAC FastAPI in production Kubernetes environments. Adapt the configurations based on your specific requirements and infrastructure constraints.