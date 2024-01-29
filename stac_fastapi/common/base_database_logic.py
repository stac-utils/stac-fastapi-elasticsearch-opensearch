from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple, Union
from stac_fastapi.types.stac import Collection, Item

class BaseDatabaseLogic(ABC):
    """
    Abstract base class for database logic.
    This class defines the interface for database operations.
    """

    @abstractmethod
    async def get_all_collections(self, token: Optional[str], limit: int) -> Any:
        pass

    @abstractmethod
    async def get_one_item(self, collection_id: str, item_id: str) -> Dict:
        pass

    @abstractmethod
    async def create_item(self, item: Item, refresh: bool = False) -> None:
        pass

    @abstractmethod
    async def delete_item(self, item_id: str, collection_id: str, refresh: bool = False) -> None:
        pass

    @abstractmethod
    async def create_collection(self, collection: Collection, refresh: bool = False) -> None:
        pass

    @abstractmethod
    async def find_collection(self, collection_id: str) -> Collection:
        pass

    @abstractmethod
    async def delete_collection(self, collection_id: str, refresh: bool = False) -> None:
        pass