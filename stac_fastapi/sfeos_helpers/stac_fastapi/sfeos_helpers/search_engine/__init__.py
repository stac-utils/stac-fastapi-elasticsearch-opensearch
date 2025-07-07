"""Search engine index management package."""

from .adapters import (
    ElasticsearchAdapter,
    OpenSearchAdapter,
    SearchEngineAdapter,
    SearchEngineAdapterFactory,
)
from .async_inserters import AsyncDatetimeIndexInserter, AsyncSimpleIndexInserter
from .base import BaseAsyncIndexInserter, BaseIndexInserter, BaseSyncIndexInserter
from .factory import IndexInsertionFactory
from .managers import DatetimeIndexManager, IndexSizeManager
from .selection import (
    AsyncDatetimeBasedIndexSelector,
    IndexSelectionStrategy,
    IndexSelectorFactory,
    SyncDatetimeBasedIndexSelector,
    UnfilteredIndexSelector,
)
from .sync_inserters import SyncDatetimeIndexInserter, SyncSimpleIndexInserter
from .types import SearchEngineType

__all__ = [
    "SearchEngineType",
    "BaseIndexInserter",
    "BaseAsyncIndexInserter",
    "BaseSyncIndexInserter",
    "SearchEngineAdapter",
    "ElasticsearchAdapter",
    "OpenSearchAdapter",
    "SearchEngineAdapterFactory",
    "IndexSizeManager",
    "DatetimeIndexManager",
    "AsyncDatetimeIndexInserter",
    "AsyncSimpleIndexInserter",
    "SyncDatetimeIndexInserter",
    "SyncSimpleIndexInserter",
    "IndexInsertionFactory",
    "IndexSelectionStrategy",
    "AsyncDatetimeBasedIndexSelector",
    "SyncDatetimeBasedIndexSelector",
    "UnfilteredIndexSelector",
    "IndexSelectorFactory",
]
