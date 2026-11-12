"""patch helpers."""

import re
from typing import Any

from pydantic import BaseModel, ConfigDict, model_validator

regex = re.compile(r"([^.' ]*:[^.'[ ]*)\.?")
replacements = str.maketrans({"/": "", ".": "", ":": "", "[": "", "]": ""})
painless_string_escapes = str.maketrans({"\\": "\\\\", "'": "\\'"})


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

    path: str | None = None
    key: str | int | None = None
    raw_key: str | None = None
    nest: str | None = None

    es_path: str | None = None
    es_key: str | None = None
    es_nest: str | None = None

    variable_name: str | None = None
    param_key: str | None = None

    model_config = ConfigDict(frozen=True)

    @model_validator(mode="before")
    @classmethod
    def validate_model(cls, data: Any):
        """Set optional fields from JSON path.

        Args:
            data (Any): input data
        """
        # Every field below lands in single-quoted Painless string literals.
        data["parts"] = (
            data["path"].lstrip("/").translate(painless_string_escapes).split("/")
        )

        data["key"] = data["parts"].pop(-1)
        data["raw_key"] = data["key"]
        data["nest"] = "/".join(data["parts"])
        data["path"] = data["nest"] + "/" + data["key"]

        data["es_key"] = data["key"]
        data["es_nest"] = "".join(
            f"[{part}]" if part.isascii() and part.isdigit() else f"['{part}']"
            for part in data["parts"]
        )
        data["es_path"] = data["es_nest"] + f"['{data['es_key']}']"

        if data["key"].lstrip("-").isdigit() or data["key"] == "-":
            data["key"] = -1 if data["key"] == "-" else int(data["key"])
            if data["raw_key"] == "-":
                # RFC 6902: "-" is the position after the last element (append).
                data["es_key"] = f"ctx._source{data['es_nest']}.size()"
            elif data["key"] < 0:
                data["es_key"] = f"ctx._source{data['es_nest']}.size() - {-data['key']}"
            else:
                data["es_key"] = str(data["key"])
            data["es_path"] = data["es_nest"] + f"[{data['es_key']}]"

        # Painless identifiers may only contain word characters and must not
        # start with a digit, so "-" in "/links/-" or "/assets/x-y.nc/title"
        # must not reach the declaration.
        variable_name = re.sub(r"\W", "_", f"{data['nest']}_{data['key']}")
        if variable_name[0].isdigit():
            variable_name = "_" + variable_name
        data["variable_name"] = variable_name
        data["param_key"] = data["path"].translate(replacements)

        return data
