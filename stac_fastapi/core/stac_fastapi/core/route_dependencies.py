"""Route Dependencies Module."""

import importlib
import inspect
import logging
import os
from typing import List

import orjson
from fastapi import Depends
from jsonschema import validate

_LOGGER = logging.getLogger("uvicorn.default")


route_dependencies_schema = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "routes": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "method": {
                            "anyOf": [
                                {"$ref": "#/$defs/method"},
                                {
                                    "type": "array",
                                    "items": {"$ref": "#/$defs/method"},
                                    "uniqueItems": True,
                                },
                            ]
                        },
                        "path": {
                            "anyOf": [
                                {"$ref": "#/$defs/path"},
                                {
                                    "type": "array",
                                    "items": {"$ref": "#/$defs/path"},
                                    "uniqueItems": True,
                                },
                            ]
                        },
                        "type": {"type": "string"},
                    },
                    "required": ["method", "path"],
                    "additionalProperties": False,
                },
            },
            "dependencies": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "method": {"type": "string"},
                        "args": {"type": "string"},
                        "kwargs": {"type": "object"},
                    },
                    "required": ["method"],
                    "additionalProperties": False,
                },
            },
        },
        "dependencies": {
            "routes": ["dependencies"],
            "dependencies": ["routes"],
        },
        "additionalProperties": False,
    },
    "$defs": {
        "method": {
            "type": "string",
            "enum": ["*", "GET", "POST", "PUT", "PATCH", "DELETE"],
        },
        "path": {
            "type": "string",
            "pattern": r"^\*$|\/.*",
        },
    },
}


def get_route_dependencies_conf(route_dependencies_env: str) -> list:
    """Get Route dependencies configuration from file or environment variable."""
    if os.path.isfile(route_dependencies_env):
        with open(route_dependencies_env, "rb") as f:
            route_dependencies_conf = orjson.loads(f.read())

    else:
        try:
            route_dependencies_conf = orjson.loads(route_dependencies_env)
        except orjson.JSONDecodeError as exception:
            _LOGGER.error("Invalid JSON format for route dependencies. %s", exception)
            raise

    validate(instance=route_dependencies_conf, schema=route_dependencies_schema)

    return route_dependencies_conf


def get_routes(route_dependency_conf: dict) -> list:
    """Get routes from route dependency configuration."""
    # seperate out any path lists
    intermediate_routes = []
    for route in route_dependency_conf["routes"]:

        if isinstance(route["path"], list):
            for path in route["path"]:
                intermediate_routes.append({**route, "path": path})

        else:
            intermediate_routes.append(route)

    # seperate out any method lists
    routes = []
    for route in intermediate_routes:

        if isinstance(route["method"], list):
            for method in route["method"]:
                routes.append({**route, "method": method})

        else:
            routes.append(route)

    return routes


def get_dependencies(route_dependency_conf: dict) -> list:
    """Get dependencies from route dependency configuration."""
    dependencies = []
    for dependency_conf in route_dependency_conf["dependencies"]:

        module_name, method_name = dependency_conf["method"].rsplit(".", 1)
        module = importlib.import_module(module_name)
        dependency = getattr(module, method_name)

        if inspect.isclass(dependency):

            dependency = dependency(
                *dependency_conf.get("args", []), **dependency_conf.get("kwargs", {})
            )

        dependencies.append(Depends(dependency))

    return dependencies


def get_route_dependencies(route_dependencies_env: str = "") -> list:
    """
    Route dependencies generator.

    Generate a set of route dependencies for authentication to the
    provided FastAPI application.
    """
    route_dependencies_env = os.environ.get(
        "STAC_FASTAPI_ROUTE_DEPENDENCIES", route_dependencies_env
    )
    route_dependencies: List[tuple] = []

    if not route_dependencies_env:
        _LOGGER.info("Authentication skipped.")
        return route_dependencies

    _LOGGER.info("Authentication enabled.")

    route_dependencies_conf = get_route_dependencies_conf(route_dependencies_env)

    for route_dependency_conf in route_dependencies_conf:

        routes = get_routes(route_dependency_conf)
        dependencies = get_dependencies(route_dependency_conf)
        route_dependencies.append((routes, dependencies))

    return route_dependencies
