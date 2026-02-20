"""CQL2 pattern conversion helpers for Elasticsearch/OpenSearch."""

import re
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from stac_fastapi.core.datetime_utils import format_datetime_range
from stac_fastapi.sfeos_helpers.mappings import ITEM_INDICES

from .ast_parser import Cql2AstParser
from .ast_transform import to_es_via_ast
from .datetime_optimizer import DatetimeOptimizer, extract_collection_datetime

cql2_like_patterns = re.compile(r"\\.|[%_]|\\$")
valid_like_substitutions = {
    "\\\\": "\\",
    "\\%": "%",
    "\\_": "_",
    "%": "*",
    "_": "?",
}


def _replace_like_patterns(match: re.Match) -> str:
    pattern = match.group()
    try:
        return valid_like_substitutions[pattern]
    except KeyError:
        raise ValueError(f"'{pattern}' is not a valid escape sequence")


def cql2_like_to_es(string: str) -> str:
    """
    Convert CQL2 "LIKE" characters to Elasticsearch "wildcard" characters.

    Args:
        string (str): The string containing CQL2 wildcard characters.

    Returns:
        str: The converted string with Elasticsearch compatible wildcards.

    Raises:
        ValueError: If an invalid escape sequence is encountered.
    """
    return cql2_like_patterns.sub(
        repl=_replace_like_patterns,
        string=string,
    )


async def resolve_cql2_indexes(
    cql2_metadata: List[Tuple[Union[str, List[str]], Optional[str]]],
    index_selector,
    apply_datetime_filter: Callable[[Any, str], Tuple[Any, Dict[str, Any]]],
    search,
    datetime_search: Optional[Dict[str, Optional[str]]] = None,
) -> Tuple[str, List[str]]:
    """Resolve indexes for CQL2 JSON queries for datetime and collection ids.

    Args:
        cql2_metadata: List of tuples containing metadata
        index_selector: The index selector instance
        apply_datetime_filter: Function to apply datetime on search
        search: The search query object
        datetime_search: Fallback datetime range from standard request parameters

    Returns:
        Comma-separated indexes, list of collection ids
    """
    import logging

    logger = logging.getLogger(__name__)

    logger.info(f"Resolving indexes from CQL2 metadata: {cql2_metadata}")

    all_collections = []
    collection_index_map: Dict[str, List[str]] = {}
    use_wildcard = False

    for collection_item, date_range in cql2_metadata:
        collections = (
            collection_item if isinstance(collection_item, list) else [collection_item]
        )
        all_collections.extend(collections)

        logger.debug(
            f"Processing collections: {collections} with date_range: {date_range}"
        )

        if date_range:
            logger.debug(f"Applying datetime filter for range: {date_range}")

            _, collection_datetime = apply_datetime_filter(
                search, format_datetime_range(date_str=date_range)
            )

            logger.debug(f"Collection datetime after filter: {collection_datetime}")

            for collection in collections:
                indexes = await index_selector.select_indexes(
                    [collection], collection_datetime
                )

                index_list = [idx.strip() for idx in indexes.split(",") if idx.strip()]

                if not index_list:
                    logger.info(
                        f"Range {date_range} returned no indexes for collection {collection}, using wildcard"
                    )
                    use_wildcard = True

                logger.debug(
                    f"Collection '{collection}' resolved to indexes: {index_list}"
                )
                collection_index_map.setdefault(collection, []).extend(index_list)
        elif datetime_search:
            # Fallback to standard datetime parameter
            collection_datetime = datetime_search
            for collection in collections:
                indexes = await index_selector.select_indexes(
                    [collection], collection_datetime
                )
                index_list = [idx.strip() for idx in indexes.split(",") if idx.strip()]
                if not index_list:
                    logger.info(
                        f"Datetime fallback returned no indexes for collection {collection}, using wildcard"
                    )
                    use_wildcard = True
                logger.debug(
                    f"Collection '{collection}' resolved to indexes (datetime fallback): {index_list}"
                )
                collection_index_map.setdefault(collection, []).extend(index_list)
        else:
            logger.info(f"No date range for collections {collections}, using wildcard")
            use_wildcard = True

    if use_wildcard:
        logger.info(
            f"At least one range requires wildcard search, using default: {ITEM_INDICES}"
        )
        return ITEM_INDICES, list(set(all_collections))

    all_indexes = []
    seen_indexes = set()

    for collection in all_collections:
        if collection in collection_index_map:
            for idx in collection_index_map[collection]:
                if idx not in seen_indexes:
                    seen_indexes.add(idx)
                    all_indexes.append(idx)

    index_param = ",".join(all_indexes)
    collection_ids = list(set(all_collections))

    logger.info(f"Final resolved indexes: {index_param or ITEM_INDICES}")
    logger.info(f"Final collection IDs: {collection_ids}")

    if not index_param:
        logger.info(f"No indexes resolved, using default: {ITEM_INDICES}")
        return ITEM_INDICES, collection_ids

    return index_param, collection_ids


def build_cql2_filter(filter: Dict) -> Tuple[Dict, List]:
    """Build query from CQL2 filter with metadata extraction.

    Args:
        filter: CQL2 JSON filter dictionary

    Returns:
        Tuple of es_query_dict, metadata
    """
    import logging

    logger = logging.getLogger(__name__)

    logger.info(f"Filter to be processed: {filter}")

    parser = Cql2AstParser()
    ast = parser.parse(filter)

    logger.debug(f"Parsed AST: {ast}")

    optimizer = DatetimeOptimizer()
    optimized_ast = optimizer.optimize_query_structure(ast)

    logger.debug(f"Optimized AST: {optimized_ast}")

    es_query = to_es_via_ast(optimized_ast)
    metadata = extract_collection_datetime(optimized_ast)

    logger.debug(f"Metadata: {metadata}")

    return es_query, metadata
