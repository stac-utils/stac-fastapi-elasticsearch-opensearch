# SFEOS Tools

CLI tools for managing [stac-fastapi-elasticsearch-opensearch](https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch) deployments.

## Installation

### For Elasticsearch

```bash
pip install sfeos-tools[elasticsearch]
```

Or for local development:
```bash
pip install -e sfeos_tools[elasticsearch]
```

### For OpenSearch

```bash
pip install sfeos-tools[opensearch]
```

Or for local development:
```bash
pip install -e sfeos_tools[opensearch]
```

### For Development (both backends)

```bash
pip install sfeos-tools[dev]
```

Or for local development:
```bash
pip install -e sfeos_tools[dev]
```

## Usage

After installation, the `sfeos-tools` command will be available:

```bash
# View available commands
sfeos-tools --help

# View version
sfeos-tools --version

# Get help for a specific command
sfeos-tools add-bbox-shape --help
```

## Commands

### add-bbox-shape

Add `bbox_shape` field to existing collections for spatial search support.

**Basic usage:**

```bash
# Elasticsearch
sfeos-tools add-bbox-shape --backend elasticsearch

# OpenSearch
sfeos-tools add-bbox-shape --backend opensearch
```

**Connection options:**

```bash
# Local Docker Compose (no SSL)
sfeos-tools add-bbox-shape --backend elasticsearch --no-ssl

# Remote server with SSL
sfeos-tools add-bbox-shape \
  --backend elasticsearch \
  --host db.example.com \
  --port 9200 \
  --user admin \
  --password secret

# Using environment variables
ES_HOST=my-cluster.cloud.com ES_PORT=9243 ES_USER=elastic ES_PASS=changeme \
  sfeos-tools add-bbox-shape --backend elasticsearch
```

**Available options:**

- `--backend`: Database backend (elasticsearch or opensearch) - **required**
- `--host`: Database host (default: localhost or ES_HOST env var)
- `--port`: Database port (default: 9200 or ES_PORT env var)
- `--use-ssl / --no-ssl`: Use SSL connection (default: true or ES_USE_SSL env var)
- `--user`: Database username (default: ES_USER env var)
- `--password`: Database password (default: ES_PASS env var)

## Development

To develop sfeos-tools locally:

```bash
# Install in editable mode with dev dependencies
pip install -e ./sfeos_tools[dev]

# Run the CLI
sfeos-tools --help
```

## License

MIT License - see the main repository for details.
