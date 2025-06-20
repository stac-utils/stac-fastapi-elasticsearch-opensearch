"""stac_fastapi: core elasticsearch/ opensearch module."""

from setuptools import find_namespace_packages, setup

with open("README.md") as f:
    desc = f.read()

install_requires = [
    "fastapi~=0.109.0",
    "attrs>=23.2.0",
    "pydantic>=2.4.1,<3.0.0",
    "stac_pydantic~=3.3.0",
    "stac-fastapi.types==6.0.0",
    "stac-fastapi.api==6.0.0",
    "stac-fastapi.extensions==6.0.0",
    "orjson~=3.9.0",
    "overrides~=7.4.0",
    "geojson-pydantic~=1.0.0",
    "pygeofilter~=0.3.1",
    "jsonschema~=4.0.0",
    "slowapi~=0.1.9",
]

setup(
    name="stac_fastapi_core",
    description="Core library for the Elasticsearch and Opensearch stac-fastapi backends.",
    long_description=desc,
    long_description_content_type="text/markdown",
    python_requires=">=3.9",
    classifiers=[
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "License :: OSI Approved :: MIT License",
    ],
    url="https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch",
    license="MIT",
    packages=find_namespace_packages(),
    zip_safe=False,
    install_requires=install_requires,
)
