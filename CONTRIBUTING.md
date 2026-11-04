# Contributing

Issues and pull requests are more than welcome.
  

### Development Environment Setup

To install the classes in your local Python env, run:

```shell
pip install -e 'stac_fastapi/elasticsearch[dev]'
```

or

```shell
pip install -e 'stac_fastapi/opensearch[dev]'
```

### Pre-commit

Install [pre-commit](https://pre-commit.com/#install).

Prior to commit, run:

```shell
pre-commit install
pre-commit run --all-files
```   

### Testing

```shell
make test
```
Test against OpenSearch only

```shell
make test-opensearch
```

Test against Elasticsearch only

```shell
make test-elasticsearch
```  

### Docs

```shell
make docs
```

Hot-reloading docs locally:

```shell
mkdocs serve -f docs/mkdocs.yml
```

### Releasing a new version

The version is pinned in several places: the four `version.py` modules, the
`STAC_FASTAPI_VERSION` fallback in each `app.py`, the inter-package `==` pins
in each package's `pyproject.toml` (used for the published wheels, not local
workspace resolution), the workspace root `pyproject.toml`, and
`helm-chart/stac-fastapi/values.yaml`.

`scripts/bump_version.sh` updates all of them, plus the `CHANGELOG.md`
section header and compare links:

```shell
./scripts/bump_version.sh 8.2.0
uv lock
```

The script is idempotent — re-running it produces no further changes. It
prints what's left manual: the README "Latest News" headline and
`helm-chart/stac-fastapi/Chart.yaml` (the chart has its own semver). Then
commit, open the release PR, and tag `vX.Y.Z` after merge.
