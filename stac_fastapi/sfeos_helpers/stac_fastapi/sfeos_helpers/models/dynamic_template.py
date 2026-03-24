"""Pydantic models for validating dynamic templates in Elasticsearch/OpenSearch mappings."""

from typing import Dict, List

from pydantic import BaseModel, field_validator, model_validator


class MappingConfig(BaseModel):
    """mapping model."""

    type: str


class DynamicTemplateDefinition(BaseModel):
    """Dynamic Template model."""

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
        for item in v:
            if len(item) != 1:
                raise ValueError("Each template must have exactly one key")
        return v
