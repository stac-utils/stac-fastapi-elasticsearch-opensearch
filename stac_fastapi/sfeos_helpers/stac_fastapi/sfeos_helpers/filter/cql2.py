"""CQL2 pattern conversion helpers for Elasticsearch/OpenSearch."""

import logging
import re
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from stac_fastapi.core.datetime_utils import format_datetime_range

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
) -> Tuple[str, List[str]]:
    """Resolve indexes for CQL2 JSON queries for datetime, collection ids.

    Args:
        cql2_metadata: List of metadata for index selection
        index_selector: The index selector instance
        apply_datetime_filter: Function to apply datetime on search
        search: The search query object

    Returns:
        Comma-separated indexes, list of collection ids
    """
    logger = logging.getLogger(__name__)

    logger.info(f"Resolving indexes from CQL2 metadata: {cql2_metadata}")

    all_collections = []
    all_indexes_set = set()

    for collection_item, date_range in cql2_metadata:
        collections = (
            collection_item if isinstance(collection_item, list) else [collection_item]
        )
        all_collections.extend(collections)

        logger.debug(
            f"Processing collections: {collections} with date_range: {date_range}"
        )

        collection_datetime = None
        logger.debug(f"Applying datetime filter for range: {date_range}")

        _, collection_datetime = apply_datetime_filter(
            search, format_datetime_range(date_str=date_range)
        )

        logger.debug(f"Collection datetime after filter: {collection_datetime}")

        if not collections or (len(collections) == 1 and not collections[0]):
            if collection_datetime:
                logger.info(
                    "No collections specified, selecting all indexes matching datetime range"
                )
                indexes = await index_selector.select_indexes([], collection_datetime)
                index_list = [idx.strip() for idx in indexes.split(",") if idx.strip()]
                logger.debug(f"All indexes matching datetime resolved to: {index_list}")

                all_indexes_set.update(index_list)
            continue

        for collection in collections:
            indexes = await index_selector.select_indexes(
                [collection], collection_datetime
            )

            index_list = [idx.strip() for idx in indexes.split(",") if idx.strip()]

            logger.debug(f"Collection '{collection}' resolved to indexes: {index_list}")

            all_indexes_set.update(index_list)

    if not all_indexes_set:
        logger.info("No indexes resolved, returning empty")
        return "", list(set(all_collections))

    index_param = ",".join(sorted(all_indexes_set))
    collection_ids = list(set(all_collections))

    logger.info(f"Final resolved indexes: {index_param}")
    logger.info(f"Final collection IDs: {collection_ids}")

    return index_param, collection_ids


def build_cql2_filter(
    queryables_mapping: Dict,
    filter: Dict,
    all_collection_ids: Optional[List[str]] = None,
) -> Tuple[Dict, List]:
    """Build query from CQL2 filter with metadata extraction.

    Args:
        filter: CQL2 JSON filter dictionary
        all_collection_ids: List of all collection IDs from database

    Returns:
        Tuple of es_query_dict, metadata
    """
    logger = logging.getLogger(__name__)

    logger.info(f"Filter to be processed: {filter}")

    parser = Cql2AstParser()
    ast = parser.parse(filter)

    logger.debug(f"Parsed AST: {ast}")

    optimizer = DatetimeOptimizer()
    optimized_ast = optimizer.optimize_query_structure(ast)

    logger.debug(f"Optimized AST: {optimized_ast}")

    es_query = to_es_via_ast(queryables_mapping, optimized_ast)

    logger.debug(f"ES query: {es_query}")

    metadata = extract_collection_datetime(optimized_ast, all_collection_ids)

    logger.debug(f"Metadata: {metadata}")

    return es_query, metadata
