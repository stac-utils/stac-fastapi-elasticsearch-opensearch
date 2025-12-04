"""STAC models."""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class Catalog(BaseModel):
    """STAC Catalog model."""

    type: str = "Catalog"
    stac_version: str
    id: str
    title: Optional[str] = None
    description: Optional[str] = None
    links: List[Dict[str, Any]]
    stac_extensions: Optional[List[str]] = None


class PartialCatalog(BaseModel):
    """Partial STAC Catalog model for updates."""

    id: str
    title: Optional[str] = None
    description: Optional[str] = None
    links: Optional[List[Dict[str, Any]]] = None
    stac_extensions: Optional[List[str]] = None
