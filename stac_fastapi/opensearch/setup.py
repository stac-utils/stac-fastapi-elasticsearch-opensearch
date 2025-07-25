"""stac_fastapi: opensearch module."""

from setuptools import find_namespace_packages, setup

with open("README.md") as f:
    desc = f.read()

install_requires = [
    "stac-fastapi-core==6.1.0",
    "sfeos-helpers==6.1.0",
    "opensearch-py~=2.8.0",
    "opensearch-py[async]~=2.8.0",
    "uvicorn~=0.23.0",
    "starlette>=0.35.0,<0.36.0",
]

extra_reqs = {
    "dev": [
        "pytest~=7.0.0",
        "pytest-cov~=4.0.0",
        "pytest-asyncio~=0.21.0",
        "pre-commit~=3.0.0",
        "ciso8601~=2.3.0",
        "httpx>=0.24.0,<0.28.0",
    ],
    "docs": ["mkdocs~=1.4.0", "mkdocs-material~=9.0.0", "pdocs~=1.2.0"],
    "server": ["uvicorn[standard]~=0.23.0"],
}

setup(
    name="stac_fastapi_opensearch",
    description="Opensearch stac-fastapi backend.",
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
    extras_require=extra_reqs,
    entry_points={
        "console_scripts": ["stac-fastapi-opensearch=stac_fastapi.opensearch.app:run"]
    },
)
