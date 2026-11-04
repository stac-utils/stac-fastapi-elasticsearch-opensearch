"""Sort extension request models matching the STAC Sort extension schema.

The upstream stac-pydantic sortby model requires `direction` and allows an
empty `sortby` array. The sort extension schema instead requires only `field`,
defaults `direction` to `asc`, and sets `minItems: 1` on the array.
"""

from typing import Annotated

from pydantic import BaseModel, Field
from stac_pydantic.api.extensions.sort import SortDirections
from stac_pydantic.api.extensions.sort import SortExtension as PostSortModel


class SortByField(PostSortModel):
    """A sortby entry where direction defaults to ascending, per the spec."""

    direction: SortDirections = SortDirections.asc


class SortExtensionPostRequest(BaseModel):
    """Sortby parameter for POST requests; the array must not be empty."""

    sortby: Annotated[list[SortByField], Field(min_length=1)] | None = Field(
        None,
        description="An array of property (field) names, and direction in form of '{'field': '<property_name>', 'direction':'<direction>'}'",  # noqa: E501
    )
