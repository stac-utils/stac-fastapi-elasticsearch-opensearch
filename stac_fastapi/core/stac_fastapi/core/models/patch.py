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
        """Convert JSON path to Elasticsearch script path.

        Args:
            path (str): initial JSON path
        """
        self.path = path.lstrip("/").replace("/", ".")

        self.nest, self.partition, self.key = path.rpartition(".")

        if self.key.isdigit():
            self.index = int(self.key)
            self.path = f"{self.nest}[{self.index}]"
            self.nest, self.partition, self.key = self.nest.rpartition(".")

    @computed_field  # type: ignore[misc]
    @property
    def location(self) -> str:
        """Compute location of path.

        Returns:
            str: path location
        """
        return self.nest + self.partition + self.key
