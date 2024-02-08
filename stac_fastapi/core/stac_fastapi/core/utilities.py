"""Module for geospatial processing functions.

This module contains functions for transforming geospatial coordinates,
such as converting bounding boxes to polygon representations.
"""
from typing import List


def bbox2polygon(b0: float, b1: float, b2: float, b3: float) -> List[List[List[float]]]:
    """Transform a bounding box represented by its four coordinates `b0`, `b1`, `b2`, and `b3` into a polygon.

    Args:
        b0 (float): The x-coordinate of the lower-left corner of the bounding box.
        b1 (float): The y-coordinate of the lower-left corner of the bounding box.
        b2 (float): The x-coordinate of the upper-right corner of the bounding box.
        b3 (float): The y-coordinate of the upper-right corner of the bounding box.

    Returns:
        List[List[List[float]]]: A polygon represented as a list of lists of coordinates.
    """
    return [[[b0, b1], [b2, b1], [b2, b3], [b0, b3], [b0, b1]]]
