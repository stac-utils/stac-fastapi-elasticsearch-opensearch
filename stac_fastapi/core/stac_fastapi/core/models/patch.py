"""patch helpers."""

from typing import Optional

from pydantic import BaseModel, computed_field


class ElasticPath(BaseModel):
    """Converts a JSON path to an Elasticsearch path.

    Args:
        path (str): JSON path to be converted.

    """

    path: str
    nest: Optional[str] = None
    partition: Optional[str] = None
    key: Optional[str] = None
    index: Optional[int] = None

    def __init__(self, *, path: str):
        self.path = path.lstrip("/").replace("/", ".")

        self.nest, self.partition, self.key = path.rpartition(".")

        if self.key.isdigit():
            self.index = int(self.key)
            self.path = f"{self.nest}[{self.index}]"
            self.nest, self.partition, self.key = self.nest.rpartition(".")

    @computed_field
    @property
    def location(self) -> str:
        return self.nest + self.partition + self.key
