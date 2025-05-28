"""patch helpers."""

import re
from typing import Any, Dict, Optional, Union

from pydantic import BaseModel, computed_field, model_validator

regex = re.compile(r"([^.' ]*:[^.'[ ]]*)\.?")


class ESCommandSet:
    """Uses dictionary keys to behaviour of ordered set.

    Yields:
        str: Elasticsearch commands
    """

    dict_: Dict[str, None] = {}

    def __init__(self):
        """Initialise ESCommandSet instance."""
        self.dict_ = {}

    def add(self, value: str):
        """Add command.

        Args:
            value (str): value to be added
        """
        self.dict_[value] = None

    def remove(self, value: str):
        """Remove command.

        Args:
            value (str): value to be removed
        """
        del self.dict_[value]

    def __iter__(self):
        """Iterate Elasticsearch commands.

        Yields:
            str: Elasticsearch command
        """
        yield from self.dict_.keys()


def to_es(string: str):
    """Convert patch operation key to Elasticsearch key.

    Args:
        string (str): string to be converted

    Returns:
        _type_: converted string
    """
    if matches := regex.findall(string):
        for match in set(matches):
            string = re.sub(rf"\.?{match}", f"['{match}']", string)

    return string


class ElasticPath(BaseModel):
    """Converts a JSON path to an Elasticsearch path.

    Args:
        path (str): JSON path to be converted.

    """

    path: str
    nest: Optional[str] = None
    partition: Optional[str] = None
    key: Optional[str] = None

    es_path: Optional[str] = None
    es_nest: Optional[str] = None
    es_key: Optional[str] = None

    index_: Optional[int] = None

    @model_validator(mode="before")
    @classmethod
    def validate_model(cls, data: Any):
        """Set optional fields from JSON path.

        Args:
            data (Any): input data
        """
        data["path"] = data["path"].lstrip("/").replace("/", ".")
        data["nest"], data["partition"], data["key"] = data["path"].rpartition(".")

        if data["key"].lstrip("-").isdigit() or data["key"] == "-":
            data["index_"] = -1 if data["key"] == "-" else int(data["key"])
            data["path"] = f"{data['nest']}[{data['index_']}]"
            data["nest"], data["partition"], data["key"] = data["nest"].rpartition(".")

        data["es_path"] = to_es(data["path"])
        data["es_nest"] = to_es(data["nest"])
        data["es_key"] = to_es(data["key"])

        return data

    @computed_field  # type: ignore[misc]
    @property
    def index(self) -> Union[int, str, None]:
        """Compute location of path.

        Returns:
            str: path index
        """
        if self.index_ and self.index_ < 0:

            return f"ctx._source.{self.location}.size() - {-self.index_}"

        return self.index_

    @computed_field  # type: ignore[misc]
    @property
    def location(self) -> str:
        """Compute location of path.

        Returns:
            str: path location
        """
        return self.nest + self.partition + self.key

    @computed_field  # type: ignore[misc]
    @property
    def es_location(self) -> str:
        """Compute location of path.

        Returns:
            str: path location
        """
        if self.es_key and ":" in self.es_key:
            return self.es_nest + self.es_key
        return self.es_nest + self.partition + self.es_key

    @computed_field  # type: ignore[misc]
    @property
    def variable_name(self) -> str:
        """Variable name for scripting.

        Returns:
            str: variable name
        """
        if self.index is not None:
            return f"{self.location.replace('.','_').replace(':','_')}_{self.index}"

        return (
            f"{self.nest.replace('.','_').replace(':','_')}_{self.key.replace(':','_')}"
        )
