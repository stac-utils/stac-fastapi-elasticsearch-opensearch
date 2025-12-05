# STAC FastAPI Helm Chart

This Helm chart deploys the STAC FastAPI application with support for both Elasticsearch and OpenSearch backends.

## Overview

The chart provides a flexible deployment solution for STAC FastAPI with the following features:

- **Dual Backend Support**: Choose between Elasticsearch or OpenSearch
- **Bundled or External Database**: Deploy with bundled database or connect to external clusters
- **Production Ready**: Includes monitoring, autoscaling, and security configurations
- **High Availability**: Support for multi-replica deployments with proper disruption budgets
- **Performance Optimized**: Configurable performance settings for large-scale deployments

## Prerequisites

- Kubernetes 1.16+
- Helm 3.0+
- Storage class for persistent volumes (if using bundled databases)

## Quick Start

### Add Helm repositories

```bash
helm repo add elasticsearch https://helm.elastic.co
helm repo add opensearch https://opensearch-project.github.io/helm-charts/
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update
```

### Deploy with Elasticsearch

```bash
# Download chart dependencies
helm dependency update ./helm-chart/stac-fastapi

# Install with Elasticsearch backend
helm install my-stac-api ./helm-chart/stac-fastapi \
  --set backend=elasticsearch \
  --set elasticsearch.enabled=true \
  --set opensearch.enabled=false
```

### Deploy with OpenSearch

```bash
# Download chart dependencies
helm dependency update ./helm-chart/stac-fastapi

# Install with OpenSearch backend
helm install my-stac-api ./helm-chart/stac-fastapi \
  --set backend=opensearch \
  --set elasticsearch.enabled=false \
  --set opensearch.enabled=true
```

### Deploy with External Database

```bash
# Create secret for API key (if needed)
kubectl create secret generic elasticsearch-api-key \
  --from-literal=api-key="your-api-key-here"

# Install with external database
helm install my-stac-api ./helm-chart/stac-fastapi \
  --values ./helm-chart/stac-fastapi/values-external.yaml \
  --set externalDatabase.host="your-database-host" \
  --set externalDatabase.port=9200
```

## Configuration

### Global Options

```yaml
global:
  imageRegistry: ""
  storageClass: ""
  clusterDomain: "cluster.local"
```

The chart builds fully qualified service endpoints for bundled databases using the Kubernetes cluster domain. Adjust `clusterDomain` if your cluster doesn't use the default `cluster.local` suffix.

## Backend Selection

The chart supports both Elasticsearch and OpenSearch backends, but only deploys **one backend at a time** based on the `backend` configuration:

### Elasticsearch Backend

```yaml
backend: elasticsearch
elasticsearch:
  enabled: true
opensearch:
  enabled: false
```

### OpenSearch Backend  

```yaml
backend: opensearch
elasticsearch:
  enabled: false
opensearch:
  enabled: true
```

### How It Works

1. **Chart Dependencies**: Both Elasticsearch and OpenSearch charts are listed as dependencies with conditions
2. **Conditional Deployment**: Only the backend specified by `backend` value is enabled
3. **Resource Isolation**: When deploying with elasticsearch, no OpenSearch resources are created (and vice versa)
4. **Automatic Configuration**: The application automatically connects to the correct backend service

### Values Files

Use the provided values files for easy backend selection:

- **Elasticsearch**: `helm install stac-fastapi ./stac-fastapi -f values-elasticsearch.yaml`
- **OpenSearch**: `helm install stac-fastapi ./stac-fastapi -f values-opensearch.yaml`

This ensures efficient resource usage and prevents conflicts between backends.

### Application Configuration

Key application settings:

```yaml
app:
  replicaCount: 2
  
  image:
    repository: ghcr.io/stac-utils/stac-fastapi
    tag: "latest"
    pullPolicy: IfNotPresent
  
  waitForDatabase:
    enabled: true
    intervalSeconds: 2
    maxAttempts: 120
  
  env:
    STAC_FASTAPI_TITLE: "STAC API"
    STAC_FASTAPI_DESCRIPTION: "A STAC FastAPI implementation"
    ENVIRONMENT: "production"
    WEB_CONCURRENCY: "10"
    ENABLE_DIRECT_RESPONSE: "false"
    DATABASE_REFRESH: "false"
    ENABLE_DATETIME_INDEX_FILTERING: "false"
    STAC_FASTAPI_RATE_LIMIT: "200/minute"
```

The optional `waitForDatabase` block adds a lightweight init container that blocks STAC FastAPI startup until the backing Elasticsearch/OpenSearch service is reachable—mirroring the docker-compose `wait-for-it` helper. Disable it by setting `app.waitForDatabase.enabled=false` if you prefer the application to start immediately and rely on internal retries instead.

### Database Configuration

#### Application Credentials

If your Elasticsearch or OpenSearch cluster requires authentication, provide credentials to the application with the `app.databaseAuth` block. You can reference an existing secret or supply literal values:

```yaml
app:
  databaseAuth:
    existingSecret: "stac-opensearch-admin"  # Optional. When set, keys are read from this secret.
    usernameKey: "username"                  # Secret key that stores the username (defaults to "username").
    passwordKey: "password"                  # Secret key that stores the password (defaults to "password").
    # username: "admin"                      # Optional literal username when not using a secret.
    # password: "changeme"                   # Optional literal password when not using a secret.
```

#### Bundled Elasticsearch

```yaml
elasticsearch:
  enabled: true
  clusterName: "stac-elasticsearch"
  replicas: 3
  minimumMasterNodes: 2
  
  resources:
    requests:
      cpu: "1000m"
      memory: "4Gi"
    limits:
      cpu: "2000m"
      memory: "4Gi"
  
  volumeClaimTemplate:
    accessModes: ["ReadWriteOnce"]
    resources:
      requests:
        storage: 100Gi
```

#### Bundled OpenSearch

```yaml
opensearch:
  enabled: true
  clusterName: "stac-opensearch"
  replicas: 3
  
  resources:
    requests:
      cpu: "1000m"
      memory: "4Gi"
    limits:
      cpu: "2000m"
      memory: "4Gi"
  
  persistence:
    enabled: true
    size: 100Gi
```

#### External Database

```yaml
externalDatabase:
  enabled: true
  host: "elasticsearch.example.com"
  port: 9200
  ssl: true
  verifyCerts: true
  apiKeySecret: "elasticsearch-credentials"
  apiKeySecretKey: "api-key"
```

### Ingress Configuration

```yaml
app:
  ingress:
    enabled: true
    className: "nginx"
    annotations:
      nginx.ingress.kubernetes.io/rewrite-target: /
      cert-manager.io/cluster-issuer: "letsencrypt-prod"
    hosts:
      - host: stac-api.example.com
        paths:
          - path: /
            pathType: Prefix
    tls:
      - secretName: stac-fastapi-tls
        hosts:
          - stac-api.example.com
```

### Autoscaling

```yaml
app:
  autoscaling:
    enabled: true
    minReplicas: 2
    maxReplicas: 10
    targetCPUUtilizationPercentage: 70
    targetMemoryUtilizationPercentage: 80
```

### Monitoring

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

## Performance Tuning

### For Large Datasets

Enable datetime-based index filtering for better performance with temporal queries:

```yaml
app:
  env:
    ENABLE_DATETIME_INDEX_FILTERING: "true"
    DATETIME_INDEX_MAX_SIZE_GB: "50"
```

### For Maximum Performance

Enable direct response mode (disables FastAPI dependencies):

```yaml
app:
  env:
    ENABLE_DIRECT_RESPONSE: "true"
    DATABASE_REFRESH: "false"
```

### For High Throughput

Increase worker processes and rate limits:

```yaml
app:
  env:
    WEB_CONCURRENCY: "8"
    STAC_FASTAPI_RATE_LIMIT: "1000/minute"
```

## Security

### Network Policies

Enable network policies for additional security:

```yaml
networkPolicy:
  enabled: true
  allowNamespaceCommunication: true   # allow pods in the release namespace to talk to each other
  allowDNS: true                      # keep kube-dns reachable for service discovery
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              name: nginx-ingress
      ports:
        - protocol: TCP
          port: 8080
  egress:
    - to:
        - podSelector:
            matchLabels:
              app.kubernetes.io/name: opensearch
      ports:
        - protocol: TCP
          port: 9200
```

### OpenSearch Admin Credentials

When deploying with the OpenSearch backend you can instruct the chart to generate
an initial admin password and store it in a Kubernetes secret. Enable this by
setting `opensearchSecurity.generateAdminPassword=true` (already enabled in
`values-opensearch.yaml`). The chart will create a secret named
`<release>-stac-fastapi-opensearch-admin` by default and automatically wires it to
the STAC FastAPI deployment through environment variables.

Retrieve the generated credentials with:

```bash
kubectl get secret <release>-stac-fastapi-opensearch-admin \
  -o jsonpath='{.data.username}' | base64 --decode
kubectl get secret <release>-stac-fastapi-opensearch-admin \
  -o jsonpath='{.data.password}' | base64 --decode
```

You can provide your own secret name, username key, or password key through the
`opensearchSecurity` values block if you already manage credentials externally.

### Pod Security Context

Configure security contexts:

```yaml
app:
  podSecurityContext:
    fsGroup: 2000
  
  securityContext:
    capabilities:
      drop:
      - ALL
    readOnlyRootFilesystem: true
    runAsNonRoot: true
    runAsUser: 1000
```

## High Availability

### Pod Disruption Budget

Ensure availability during maintenance:

```yaml
podDisruptionBudget:
  enabled: true
  minAvailable: 1
```

### Anti-Affinity

Spread pods across nodes:

```yaml
app:
  affinity:
    podAntiAffinity:
      preferredDuringSchedulingIgnoredDuringExecution:
      - weight: 100
        podAffinityTerm:
          labelSelector:
            matchExpressions:
            - key: app.kubernetes.io/name
              operator: In
              values:
              - stac-fastapi
          topologyKey: kubernetes.io/hostname
```

## Examples

The chart includes several example values files:

- `values-elasticsearch.yaml`: Production-ready Elasticsearch deployment
- `values-opensearch.yaml`: Production-ready OpenSearch deployment
- `values-external.yaml`: External database configuration

Use them as starting points:

```bash
helm install my-stac-api ./helm-chart/stac-fastapi \
  --values ./helm-chart/stac-fastapi/values-elasticsearch.yaml
```

## Upgrading

To upgrade an existing deployment:

```bash
helm upgrade my-stac-api ./helm-chart/stac-fastapi \
  --values your-values.yaml
```

## Uninstalling

To remove the deployment:

```bash
helm uninstall my-stac-api
```

**Note**: Persistent volumes for databases may need to be manually deleted.

## Troubleshooting

### Check Pod Status

```bash
kubectl get pods -l app.kubernetes.io/name=stac-fastapi
```

### View Logs

```bash
kubectl logs -l app.kubernetes.io/name=stac-fastapi
```

### Check Database Connectivity

```bash
kubectl exec -it deployment/my-stac-api -- curl http://elasticsearch:9200/_health
```

### Port Forward for Local Testing

```bash
kubectl port-forward service/my-stac-api 8080:80
```

Then visit <http://localhost:8080>

## Configuration Reference

| Parameter | Description | Default |
|-----------|-------------|---------|
| `backend` | Database backend (elasticsearch/opensearch) | `elasticsearch` |
| `app.replicaCount` | Number of application replicas | `2` |
| `app.image.repository` | Application image repository | `ghcr.io/stac-utils/stac-fastapi` |
| `app.image.tag` | Application image tag | `""` (uses chart appVersion) |
| `app.service.type` | Kubernetes service type | `ClusterIP` |
| `app.service.port` | Service port | `80` |
| `app.ingress.enabled` | Enable ingress | `false` |
| `app.autoscaling.enabled` | Enable horizontal pod autoscaler | `false` |
| `elasticsearch.enabled` | Deploy Elasticsearch | `true` |
| `opensearch.enabled` | Deploy OpenSearch | `false` |
| `externalDatabase.enabled` | Use external database | `false` |
| `opensearchSecurity.generateAdminPassword` | Generate random OpenSearch admin password | `false` |
| `opensearchSecurity.secretName` | Override name of generated OpenSearch admin secret | `""` |
| `monitoring.enabled` | Enable monitoring | `false` |
| `networkPolicy.enabled` | Enable network policies | `false` |
| `networkPolicy.allowNamespaceCommunication` | Allow ingress/egress within the release namespace | `true` |
| `networkPolicy.allowDNS` | Allow egress to kube-dns for service discovery | `true` |
| `podDisruptionBudget.enabled` | Enable pod disruption budget | `false` |

For a complete list of configuration options, see the `values.yaml` file.

## Contributing

Contributions are welcome! Please ensure any changes maintain compatibility with both Elasticsearch and OpenSearch backends.

## License

This chart is released under the same license as the STAC FastAPI project.
