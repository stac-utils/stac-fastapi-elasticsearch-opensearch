"""Tests for JSON Patch script parameter isolation."""

from stac_fastapi.extensions.transaction.request import (
    PatchAddReplaceTest,
    PatchMoveCopy,
    PatchRemove,
)
from stac_fastapi.sfeos_helpers.database.utils import add_commands, operations_to_script
from stac_fastapi.sfeos_helpers.database.utils import (
    test_commands as build_test_commands,
)
from stac_fastapi.sfeos_helpers.models.patch import ElasticPath, ESCommandSet


def test_operations_to_script_allocates_each_value_occurrence():
    operations = [
        PatchAddReplaceTest.model_validate(
            {"op": "add", "path": "/id", "value": "plain"}
        ),
        PatchAddReplaceTest.model_validate(
            {"op": "add", "path": "/i:d", "value": "colon"}
        ),
        PatchAddReplaceTest.model_validate(
            {"op": "add", "path": "/type", "value": "type"}
        ),
        PatchAddReplaceTest.model_validate(
            {"op": "add", "path": "/t:ype", "value": "colon-type"}
        ),
        PatchAddReplaceTest.model_validate(
            {"op": "add", "path": "/properties/name", "value": "ordinary"}
        ),
        PatchAddReplaceTest.model_validate(
            {
                "op": "add",
                "path": "/properties/n:ame",
                "value": "nested-colon",
            }
        ),
    ]

    script = operations_to_script(operations)

    assert script["params"] == {
        "p0": "plain",
        "p1": "colon",
        "p2": "type",
        "p3": "colon-type",
        "p4": "ordinary",
        "p5": "nested-colon",
    }
    for index in range(6):
        assert f"params.p{index}" in script["source"]


def test_operations_to_script_allocates_repeated_values_and_array_values():
    operations = [
        PatchAddReplaceTest.model_validate(
            {"op": "add", "path": "/properties/value", "value": "first"}
        ),
        PatchAddReplaceTest.model_validate(
            {"op": "replace", "path": "/properties/value", "value": "second"}
        ),
        PatchAddReplaceTest.model_validate(
            {"op": "test", "path": "/properties/value", "value": "second"}
        ),
        PatchAddReplaceTest.model_validate(
            {"op": "add", "path": "/properties/array/0", "value": 10}
        ),
        PatchAddReplaceTest.model_validate(
            {"op": "add", "path": "/properties/array/-", "value": 20}
        ),
        PatchAddReplaceTest.model_validate(
            {"op": "add", "path": "/properties/my-field", "value": "hyphen"}
        ),
    ]

    script = operations_to_script(operations)

    assert script["params"] == {
        "p0": "first",
        "p1": "second",
        "p2": "second",
        "p3": 10,
        "p4": 20,
        "p5": "hyphen",
    }
    assert script["source"].count("params.p0") == 1
    assert script["source"].count("params.p1") == 1
    assert script["source"].count("params.p2") == 1
    assert "params.p3" in script["source"]
    assert "params.p4" in script["source"]
    assert "params.p5" in script["source"]


def test_operations_to_script_starts_allocator_for_each_call():
    operation = PatchAddReplaceTest.model_validate(
        {"op": "add", "path": "/properties/value", "value": "value"}
    )

    first = operations_to_script([operation])
    second = operations_to_script([operation])

    assert first["params"] == {"p0": "value"}
    assert second["params"] == {"p0": "value"}


def test_value_helpers_skip_prepopulated_parameter_keys():
    operation = PatchAddReplaceTest.model_validate(
        {"op": "add", "path": "/properties/value", "value": "value"}
    )
    path = ElasticPath(path=operation.path)
    commands = ESCommandSet()
    params = {"p0": "occupied", "p2": "also occupied"}

    add_commands(commands, operation, path, None, params)

    test_operation = PatchAddReplaceTest.model_validate(
        {"op": "test", "path": "/properties/other", "value": "other"}
    )
    test_path = ElasticPath(path=test_operation.path)
    build_test_commands(commands, test_operation, test_path, params)

    assert params == {
        "p0": "occupied",
        "p1": "value",
        "p2": "also occupied",
        "p3": "other",
    }
    source = "".join(commands)
    assert "params.p1" in source
    assert "params.p3" in source


def test_operations_to_script_keeps_non_value_operation_handling():
    operations = [
        PatchAddReplaceTest.model_validate(
            {"op": "add", "path": "/properties/source", "value": "value"}
        ),
        PatchAddReplaceTest.model_validate(
            {"op": "replace", "path": "/properties/source", "value": "updated"}
        ),
        PatchMoveCopy.model_validate(
            {"op": "copy", "path": "/properties/copied", "from": "/properties/source"}
        ),
        PatchMoveCopy.model_validate(
            {"op": "move", "path": "/properties/moved", "from": "/properties/source"}
        ),
        PatchRemove.model_validate({"op": "remove", "path": "/properties/moved"}),
    ]

    script = operations_to_script(operations)

    assert script["params"] == {"p0": "value", "p1": "updated"}
    assert "ctx._source['properties']['source'] = params.p0;" in script["source"]
    assert "ctx._source['properties']['source'] = params.p1;" in script["source"]
    assert (
        "ctx._source['properties']['copied'] = ctx._source['properties']['source'];"
        in script["source"]
    )
    assert ElasticPath(path="/properties/proj:epsg").param_key == "propertiesprojepsg"


def test_repeated_same_operation_keeps_every_binding():
    for op in ("add", "replace", "test"):
        for values in (("first", "second"), ("same", "same")):
            operations = [
                PatchAddReplaceTest.model_validate(
                    {"op": op, "path": "/properties/value", "value": value}
                )
                for value in values
            ]

            script = operations_to_script(operations)

            assert script["params"] == dict(zip(("p0", "p1"), values))
            assert "params.p0" in script["source"]
            assert "params.p1" in script["source"]
