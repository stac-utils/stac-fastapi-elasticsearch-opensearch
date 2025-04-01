"""patch helpers."""

from typing import Any, Optional, Union

from pydantic import BaseModel, computed_field, model_validator


class ElasticPath(BaseModel):
    """Converts a JSON path to an Elasticsearch path.

    Args:
        path (str): JSON path to be converted.

    """

    path: str
    nest: Optional[str] = None
    partition: Optional[str] = None
    key: Optional[str] = None
    _index: Optional[int] = None

    @model_validator(mode="before")
    @classmethod
    def validate_model(cls, data: Any):
        """Set optional fields from JSON path.

        Args:
            data (Any): input data
        """
        data["path"] = data["path"].lstrip("/").replace("/", ".")

        data["nest"], data["partition"], data["key"] = data["path"].rpartition(".")

        if data["key"].isdigit() or data["key"] == "-":
            data["_index"] = -1 if data["key"] == "-" else int(data["key"])
            data["nest"], data["partition"], data["key"] = data["nest"].rpartition(".")
            data["path"] = f"{data['nest']}[{data['_index']}]"

        return data

    @property
    def index(self) -> Union[int, str, None]:
        """Compute location of path.

        Returns:
            str: path location
        """
        if self._index and self._index < 0:

            return f"ctx._source.{self.location}.size() - {-self._index}"

        return self._index

    @computed_field  # type: ignore[misc]
    @property
    def location(self) -> str:
        """Compute location of path.

        Returns:
            str: path location
        """
        return self.nest + self.partition + self.key
