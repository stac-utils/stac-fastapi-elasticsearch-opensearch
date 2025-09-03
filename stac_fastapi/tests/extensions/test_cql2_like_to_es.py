import pytest

from stac_fastapi.sfeos_helpers.filter import cql2_like_to_es


@pytest.mark.parametrize(
    "cql2_value, expected_es_value",
    (
        # no-op
        ("", ""),
        # backslash
        ("\\\\", "\\"),
        # percent
        ("%", "*"),
        (r"\%", "%"),
        (r"\\%", r"\*"),
        (r"\\\%", r"\%"),
        # underscore
        ("_", "?"),
        (r"\_", "_"),
        (r"\\_", r"\?"),
        (r"\\\_", r"\_"),
    ),
)
def test_cql2_like_to_es_success(cql2_value: str, expected_es_value: str) -> None:
    """Verify CQL2 LIKE query strings are converted correctly."""

    assert cql2_like_to_es(cql2_value) == expected_es_value


@pytest.mark.parametrize(
    "cql2_value",
    (
        pytest.param("\\", id="trailing backslash escape"),
        pytest.param("\\1", id="invalid escape sequence"),
    ),
)
def test_cql2_like_to_es_invalid(cql2_value: str) -> None:
    """Verify that incomplete or invalid escape sequences are rejected.

    CQL2 currently doesn't appear to define how to handle invalid escape sequences.
    This test assumes that undefined behavior is caught.
    """

    with pytest.raises(ValueError):
        cql2_like_to_es(cql2_value)
