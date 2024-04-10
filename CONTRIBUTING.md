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
