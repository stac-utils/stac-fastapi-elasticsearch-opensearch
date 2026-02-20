"""STAC models."""

from typing import Any

from pydantic import BaseModel


class Catalog(BaseModel):
    """STAC Catalog model."""

    type: str = "Catalog"
    stac_version: str
    id: str
    title: str | None = None
    description: str | None = None
    links: list[dict[str, Any]]
    stac_extensions: list[str] | None = None


class PartialCatalog(BaseModel):
    """Partial STAC Catalog model for updates."""

    id: str
    title: str | None = None
    description: str | None = None
    links: list[dict[str, Any]] | None = None
    stac_extensions: list[str] | None = None
