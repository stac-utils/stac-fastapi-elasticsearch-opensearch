"""`is_hidden` decides as the search filter from `add_hidden_filter` does."""

import pytest

from stac_fastapi.sfeos_helpers.database.utils import is_hidden

PATH = "properties._private.hidden"


@pytest.mark.parametrize(
    "source, hidden",
    [
        ({}, False),
        ({"properties": {"_private": {"hidden": False}}}, False),
        ({"properties": {"_private": {"hidden": True}}}, True),
        ({"properties": {"_private": {"hidden": "false"}}}, False),
        ({"properties": {"_private": {"hidden": ""}}}, False),
        ({"properties": {"_private": {"hidden": "true"}}}, True),
        ({"properties": {"_private": {"hidden": [False]}}}, False),
        ({"properties": {"_private": {"hidden": [True, False]}}}, False),
        ({"properties": {"_private": {"hidden": [True]}}}, True),
        ({"properties": {"_private": {"hidden": []}}}, False),
        ({"properties": {"_private": {"hidden": None}}}, False),
        ({"properties": {"_private.hidden": True}}, True),
        ({"properties._private.hidden": True}, True),
        ({"properties._private": {"hidden": False}}, False),
        ({"properties": {"_private": "not an object"}}, False),
        ({"properties": {"_private": [{"hidden": True}]}}, True),
        ({"properties": {"_private": [{"hidden": True}, {"hidden": False}]}}, False),
        ({"properties": {"_private": {"hidden": None}, "_private.hidden": True}}, True),
        (
            {"properties": {"_private": {"hidden": True}, "_private.hidden": False}},
            False,
        ),
        ({"properties": {"_private": {"hidden": [[False]]}}}, False),
        ({"properties": {"_private": {"hidden": [[True]]}}}, True),
        ({"properties": [{"_private": {"hidden": True}}]}, True),
    ],
)
def test_is_hidden_matches_the_search_filter(source, hidden):
    assert is_hidden(source, PATH) is hidden


def test_no_path_hides_nothing():
    assert is_hidden({"properties": {"_private": {"hidden": True}}}, None) is False
