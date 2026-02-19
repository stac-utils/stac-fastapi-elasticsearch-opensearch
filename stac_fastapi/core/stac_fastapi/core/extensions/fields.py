"""Fields extension."""

from typing import Optional

from pydantic import BaseModel, Field

from stac_fastapi.extensions.core import FieldsExtension as FieldsExtensionBase
from stac_fastapi.extensions.core.fields import request


class PostFieldsExtension(request.PostFieldsExtension):
    """PostFieldsExtension."""

    # Set defaults if needed
    # include : Optional[Set[str]] = Field(
    #    default_factory=lambda: {
    #         "id",
    #         "type",
    #         "stac_version",
    #         "geometry",
    #         "bbox",
    #         "links",
    #         "assets",
    #         "properties.datetime",
    #         "collection",
    #     }
    # )
    include: Optional[set[str]] = set()
    exclude: Optional[set[str]] = set()


class FieldsExtensionPostRequest(BaseModel):
    """Additional fields and schema for the POST request."""

    fields: Optional[PostFieldsExtension] = Field(PostFieldsExtension())


class FieldsExtension(FieldsExtensionBase):
    """Override the POST model."""

    POST = FieldsExtensionPostRequest
