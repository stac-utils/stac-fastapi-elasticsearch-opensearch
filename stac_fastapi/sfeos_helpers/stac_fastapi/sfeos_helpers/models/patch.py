"""patch helpers."""

import re
from typing import Any, Dict, Optional, Union

from pydantic import BaseModel, model_validator

regex = re.compile(r"([^.' ]*:[^.'[ ]*)\.?")
replacements = str.maketrans({"/": "", ".": "", ":": "", "[": "", "]": ""})


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

    parts: list[str] = []

    path: Optional[str] = None
    key: Optional[Union[str, int]] = None
    nest: Optional[str] = None

    es_path: Optional[str] = None
    es_key: Optional[str] = None
    es_nest: Optional[str] = None

    variable_name: Optional[str] = None
    param_key: Optional[str] = None

    class Config:
        """Class config."""

        frozen = True

    @model_validator(mode="before")
    @classmethod
    def validate_model(cls, data: Any):
        """Set optional fields from JSON path.

        Args:
            data (Any): input data
        """
        data["parts"] = data["path"].lstrip("/").split("/")

        data["key"] = data["parts"].pop(-1)
        data["nest"] = "/".join(data["parts"])
        data["path"] = data["nest"] + "/" + data["key"]

        data["es_key"] = data["key"]
        data["es_nest"] = "".join([f"['{part}']" for part in data["parts"]])
        data["es_path"] = data["es_nest"] + f"['{data['es_key']}']"

        if data["key"].lstrip("-").isdigit() or data["key"] == "-":
            data["key"] = -1 if data["key"] == "-" else int(data["key"])
            data["es_key"] = (
                f"ctx._source{data['es_nest']}.size() - {-data['key']}"
                if data["key"] < 0
                else str(data["key"])
            )
            data["es_path"] = data["es_nest"] + f"[{data['es_key']}]"

        data[
            "variable_name"
        ] = f"{data['nest'].replace('/','_').replace(':','_')}_{str(data['key']).replace(':','_')}"
        data["param_key"] = data["path"].translate(replacements)

        return data
