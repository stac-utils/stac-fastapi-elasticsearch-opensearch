"""stac_fastapi: elasticsearch module."""

from setuptools import find_namespace_packages, setup

with open("README.md") as f:
    desc = f.read()

install_requires = [
    "stac_fastapi_core==4.0.0a1",
    "elasticsearch[async]==8.11.0",
    "elasticsearch-dsl==8.11.0",
    "uvicorn",
    "starlette",
]

extra_reqs = {
    "dev": [
        "pytest",
        "pytest-cov",
        "pytest-asyncio",
        "pre-commit",
        "requests",
        "ciso8601",
        "httpx<=0.27.2",
    ],
    "docs": ["mkdocs", "mkdocs-material", "pdocs"],
    "server": ["uvicorn[standard]==0.19.0"],
}

setup(
    name="stac_fastapi_elasticsearch",
    description="An implementation of STAC API based on the FastAPI framework with both Elasticsearch and Opensearch.",
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
    packages=find_namespace_packages(exclude=["alembic", "tests", "scripts"]),
    zip_safe=False,
    install_requires=install_requires,
    tests_require=extra_reqs["dev"],
    extras_require=extra_reqs,
    entry_points={
        "console_scripts": [
            "stac-fastapi-elasticsearch=stac_fastapi.elasticsearch.app:run"
        ]
    },
)
