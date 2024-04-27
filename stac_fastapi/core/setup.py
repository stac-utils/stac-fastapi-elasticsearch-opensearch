"""stac_fastapi: core elasticsearch/ opensearch module."""

from setuptools import find_namespace_packages, setup

with open("README.md") as f:
    desc = f.read()

install_requires = [
    "fastapi",
    "attrs>=23.2.0",
    "pydantic[dotenv]",
    "stac_pydantic>=3",
    # "stac-fastapi.types==2.5.3",
    # "stac-fastapi.api==2.5.3",
    # "stac-fastapi.extensions==2.5.3",

    # use latest commits
    "stac-fastapi.api @ git+https://github.com/stac-utils/stac-fastapi/@e7f82d6996af0f28574329d57f5a5e90431d66bb#egg=stac-fastapi.api&subdirectory=stac_fastapi/api",
    "stac-fastapi.extensions @ git+https://github.com/stac-utils/stac-fastapi/@e7f82d6996af0f28574329d57f5a5e90431d66bb#egg=stac-fastapi.extensions&subdirectory=stac_fastapi/extensions",
    "stac-fastapi.types @ git+https://github.com/stac-utils/stac-fastapi/@e7f82d6996af0f28574329d57f5a5e90431d66bb#egg=stac-fastapi.types&subdirectory=stac_fastapi/types",
    "pystac[validation]",
    "orjson",
    "overrides",
    "geojson-pydantic",
    "pygeofilter==0.2.1",
]

setup(
    name="stac-fastapi.core",
    description="Core library for the Elasticsearch, Mongo, and Opensearch stac-fastapi backends.",
    long_description=desc,
    long_description_content_type="text/markdown",
    python_requires=">=3.8",
    classifiers=[
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: MIT License",
    ],
    url="https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch",
    license="MIT",
    packages=find_namespace_packages(),
    zip_safe=False,
    install_requires=install_requires,
)
