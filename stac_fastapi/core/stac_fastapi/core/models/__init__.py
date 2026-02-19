"""STAC models."""

from typing import Any, Optional

from pydantic import BaseModel


class Catalog(BaseModel):
    """STAC Catalog model."""

    type: str = "Catalog"
    stac_version: str
    id: str
    title: Optional[str] = None
    description: Optional[str] = None
    links: list[dict[str, Any]]
    stac_extensions: Optional[list[str]] = None


class PartialCatalog(BaseModel):
    """Partial STAC Catalog model for updates."""

    id: str
    title: Optional[str] = None
    description: Optional[str] = None
    links: Optional[list[dict[str, Any]]] = None
    stac_extensions: Optional[list[str]] = None
