"""Shared settings models for SFEOS backends."""

from pydantic import BaseModel


class SfeosExtensionsSettings(BaseModel):
    """SFEOS extension feature flags exposed through settings models."""

    enable_transactions_extensions: bool = True
    enable_collections_search: bool = True
    enable_collections_search_route: bool = False
    enable_catalogs_route: bool = False
    hide_alternate_parents: bool = False
