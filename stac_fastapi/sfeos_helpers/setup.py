"""stac_fastapi: helpers elasticsearch/ opensearch module."""

from setuptools import find_namespace_packages, setup

with open("README.md") as f:
    desc = f.read()

install_requires = [
    "stac-fastapi.core==6.4.0",
]

setup(
    name="sfeos_helpers",
    description="Helper library for the Elasticsearch and Opensearch stac-fastapi backends.",
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
