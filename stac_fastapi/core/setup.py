"""stac_fastapi: core elasticsearch/ opensearch module."""

from setuptools import find_namespace_packages, setup

with open("README.md") as f:
    desc = f.read()

install_requires = [
    "fastapi",
    "attrs",
    "pydantic[dotenv]<2",
    "stac_pydantic==2.0.*",
    "stac-fastapi.types==2.4.9",
    "stac-fastapi.api==2.4.9",
    "stac-fastapi.extensions==2.4.9",
    "pystac[validation]",
    "orjson",
    "overrides",
    "geojson-pydantic",
    "pygeofilter==0.2.1",
]

setup(
    name="stac-fastapi.core",
    description="Core library for the Elasticsearch and Opensearch stac-fastapi backends.",
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
