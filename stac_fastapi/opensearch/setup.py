"""stac_fastapi: elasticsearch module."""

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
    # "elasticsearch[async]==8.11.0",
    # "elasticsearch-dsl==8.11.0",
    "pystac[validation]",
    # "uvicorn",
    "orjson",
    "overrides",
    # "starlette",
    "geojson-pydantic",
    "pygeofilter==0.2.1",
]

# extra_reqs = {
#     "dev": [
#         "pytest",
#         "pytest-cov",
#         "pytest-asyncio",
#         "pre-commit",
#         "requests",
#         "ciso8601",
#         "httpx",
#     ],
#     "docs": ["mkdocs", "mkdocs-material", "pdocs"],
#     "server": ["uvicorn[standard]==0.19.0"],
# }

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
    url="https://github.com/stac-utils/stac-fastapi-elasticsearch",
    license="MIT",
    packages=find_namespace_packages(),
    zip_safe=False,
    install_requires=install_requires,
)
