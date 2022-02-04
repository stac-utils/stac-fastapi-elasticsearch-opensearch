"""stac_fastapi: mongo module."""

from setuptools import find_namespace_packages, setup

with open("README.md") as f:
    desc = f.read()

install_requires = [
    "attrs",
    "pydantic[dotenv]",
    "stac_pydantic==2.0.*",
    "stac-fastapi.types==2.3.0",
    "stac-fastapi.api==2.3.0",
    "stac-fastapi.extensions==2.3.0",
    "fastapi-utils",
    "pymongo",
    "pystac[validation]",
    "uvicorn",
]

extra_reqs = {
    "dev": [
        "pytest",
        "pytest-cov",
        "pytest-asyncio",
        "pre-commit",
        "requests",
    ],
    "docs": ["mkdocs", "mkdocs-material", "pdocs"],
    "server": ["uvicorn[standard]>=0.12.0,<0.14.0"],
}


setup(
    name="stac-fastapi.mongo",
    description="An implementation of STAC API based on the FastAPI framework.",
    long_description=desc,
    long_description_content_type="text/markdown",
    python_requires=">=3.8",
    classifiers=[
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: MIT License",
    ],
    keywords="STAC FastAPI COG",
    author=u"Arturo Engineering",
    author_email="engineering@arturo.ai",
    url="https://github.com/jonhealy1/stac-fastapi-nosql",
    license="MIT",
    packages=find_namespace_packages(exclude=["alembic", "tests", "scripts"]),
    zip_safe=False,
    install_requires=install_requires,
    tests_require=extra_reqs["dev"],
    extras_require=extra_reqs,
    entry_points={"console_scripts": ["stac-fastapi-mongo=stac_fastapi.mongo.app:run"]},
)
