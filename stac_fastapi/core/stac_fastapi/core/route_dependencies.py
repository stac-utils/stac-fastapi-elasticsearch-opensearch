"""Route Dependencies Module."""

import importlib
import inspect
import json
import logging
import os

from fastapi import Depends

_LOGGER = logging.getLogger("uvicorn.default")


def get_route_dependencies(route_dependencies_env: str = "") -> list:
    """
    Route dependencies generator.

    Generate a set of route dependencies for authentication to the
    provided FastAPI application.
    """
    route_dependencies_env = os.environ.get(
        "STAC_FASTAPI_ROUTE_DEPENDENCIES", route_dependencies_env
    )
    route_dependencies = []

    if route_dependencies_env:
        _LOGGER.info("Authentication enabled.")

        if os.path.exists(route_dependencies_env):
            with open(
                route_dependencies_env, encoding="utf-8"
            ) as route_dependencies_file:
                route_dependencies_conf = json.load(route_dependencies_file)

        else:
            try:
                route_dependencies_conf = json.loads(route_dependencies_env)
            except json.JSONDecodeError as exception:
                _LOGGER.error(
                    "Invalid JSON format for route dependencies. %s", exception
                )
                raise

        for route_dependency_conf in route_dependencies_conf:

            routes = []
            for route in route_dependency_conf["routes"]:

                if isinstance(route["method"], list):
                    for method in route["method"]:
                        route["method"] = method
                        routes.append(route)

            dependencies_conf = route_dependency_conf["dependencies"]

            dependencies = []
            for dependency_conf in dependencies_conf:

                module_name, method_name = dependency_conf["method"].rsplit(".", 1)

                module = importlib.import_module(module_name)

                dependency = getattr(module, method_name)

                if inspect.isclass(dependency):

                    dependency = dependency(
                        *dependency_conf.get("args", []),
                        **dependency_conf.get("kwargs", {})
                    )

                dependencies.append(Depends(dependency))

            route_dependencies.append((routes, dependencies))

    else:
        _LOGGER.info("Authentication skipped.")

    return route_dependencies
