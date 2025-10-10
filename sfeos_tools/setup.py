"""Setup for SFEOS Tools."""

from setuptools import find_packages, setup

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="sfeos-tools",
    version="0.1.0",
    description="CLI tools for managing stac-fastapi-elasticsearch-opensearch deployments",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Jonathan Healy",
    license="MIT",
    url="https://github.com/stac-utils/stac-fastapi-elasticsearch-opensearch",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "click>=8.0.0",
    ],
    extras_require={
        "elasticsearch": [
            "stac_fastapi_core",
            "sfeos_helpers",
            "stac_fastapi_elasticsearch",
        ],
        "opensearch": [
            "stac_fastapi_core",
            "sfeos_helpers",
            "stac_fastapi_opensearch",
        ],
        "dev": [
            "stac_fastapi_core",
            "sfeos_helpers",
            "stac_fastapi_elasticsearch",
            "stac_fastapi_opensearch",
        ],
    },
    entry_points={
        "console_scripts": [
            "sfeos-tools=sfeos_tools.cli:cli",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
)
