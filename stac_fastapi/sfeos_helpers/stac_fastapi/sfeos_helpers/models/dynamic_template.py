"""Pydantic models for validating dynamic templates in Elasticsearch/OpenSearch mappings."""

from typing import Dict, List

from pydantic import BaseModel, field_validator, model_validator


class MappingConfig(BaseModel):
    """Defines the mapping configuration applied to fields.

    This model represents the mapping settings to control how matched fields are indexed and stored.

    Attributes:
        type: The field data type (e.g., "text", "keyword", "long", "date").
    """

    type: str


class DynamicTemplateDefinition(BaseModel):
    """Defines a dynamic template.

    A dynamic template controls how fields are mapped based on
    field names, paths, or detected data types.

    Attributes:
        match_mapping_type: Match fields by detected data type.
        unmatch_mapping_type: Exclude fields by detected data type.
        match: Pattern to match field names.
        unmatch: Pattern to exclude field names.
        path_match: Pattern to match full dotted field paths.
        path_unmatch: Pattern to exclude full dotted field paths.
        mapping: Mapping configuration applied to matched fields.
    """

    match_mapping_type: str | None = None
    unmatch_mapping_type: str | None = None
    match: str | None = None
    unmatch: str | None = None
    path_match: str | None = None
    path_unmatch: str | None = None
    mapping: MappingConfig

    @model_validator(mode="after")
    def check_at_least_one(self) -> "DynamicTemplateDefinition":
        """Check that at least one of match_mapping_type, unmatch_mapping_type, match, unmatch, path_match, path_unmatch are provided."""
        fields = [
            self.match_mapping_type,
            self.unmatch_mapping_type,
            self.match,
            self.unmatch,
            self.path_match,
            self.path_unmatch,
        ]

        if not any(fields):
            raise ValueError(
                "At least one of match_mapping_type, unmatch_mapping_type, match, unmatch, path_match, path_unmatch must be provided"
            )
        return self


class DynamicTemplatesModel(BaseModel):
    """Model for validating dynamic templates."""

    templates: List[Dict[str, DynamicTemplateDefinition]]

    @field_validator("templates")
    @classmethod
    def validate_single_key_dict(cls, v):
        """Validate that each item in the templates list is a dict with exactly one key."""
        for i, item in enumerate(v):
            if len(item) != 1:
                raise ValueError(
                    f"Template at index {i} must have exactly one key, got {len(item)}"
                )

            template_name = next(iter(item.keys()))
            if not template_name or not isinstance(template_name, str):
                raise ValueError(
                    f"Template name must be a non-empty string, got {template_name}"
                )

        return v
