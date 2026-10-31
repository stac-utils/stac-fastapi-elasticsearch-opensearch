"""Tests for JSON Patch script parameter isolation."""

from stac_fastapi.extensions.transaction.request import (
    PatchAddReplaceTest,
    PatchMoveCopy,
    PatchRemove,
)
from stac_fastapi.sfeos_helpers.database.utils import (
    add_commands,
    operations_to_script,
    protect_datetime_script,
)
from stac_fastapi.sfeos_helpers.database.utils import (
    test_commands as build_test_commands,
)
from stac_fastapi.sfeos_helpers.models.patch import ElasticPath


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
    commands: list[str] = []
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
        "ctx._source['properties']['copied'] = deepCopy(ctx._source['properties']['source']);"
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


def test_repeated_non_value_operations_keep_every_statement():
    for operation in (
        {"op": "copy", "path": "/properties/a/1", "from": "/properties/a/0"},
        {"op": "move", "path": "/properties/a/1", "from": "/properties/a/0"},
        {"op": "remove", "path": "/properties/a/0"},
    ):
        model = PatchRemove if operation["op"] == "remove" else PatchMoveCopy
        once = operations_to_script([model.model_validate(operation)])["source"]
        twice = operations_to_script([model.model_validate(operation)] * 2)["source"]

        for statement in ("['a'].add(", "['a'].remove("):
            assert twice.count(statement) == 2 * once.count(statement)
        assert len(twice) > len(once)


def test_repeated_remove_declares_distinct_variables():
    remove = PatchRemove.model_validate({"op": "remove", "path": "/properties/x"})

    source = operations_to_script([remove, remove])["source"]

    assert "def properties_x_0;" in source
    assert "def properties_x_1;" in source


def test_remove_then_replace_keeps_both_existence_checks():
    operations = [
        PatchRemove.model_validate({"op": "remove", "path": "/properties/x"}),
        PatchAddReplaceTest.model_validate(
            {"op": "replace", "path": "/properties/x", "value": 2}
        ),
    ]

    source = operations_to_script(operations)["source"]

    assert source.count("containsKey('x')") == 2
    assert source.index("remove('x')") < source.rindex("containsKey('x')")


def test_deep_copy_function_only_with_copy():
    copy = PatchMoveCopy.model_validate(
        {"op": "copy", "path": "/properties/c", "from": "/properties/b"}
    )
    move = PatchMoveCopy.model_validate(
        {"op": "move", "path": "/properties/c", "from": "/properties/b"}
    )

    copy_source = operations_to_script([copy])["source"]
    move_source = operations_to_script([move])["source"]

    assert copy_source.startswith("def deepCopy(")
    assert "= deepCopy(ctx._source['properties']['b']);" in copy_source
    assert "deepCopy" not in move_source


def test_datetime_guards_follow_deep_copy_function():
    copy = PatchMoveCopy.model_validate(
        {"op": "copy", "path": "/properties/c", "from": "/properties/b"}
    )
    script = operations_to_script([copy])

    protect_datetime_script(script, True)

    source = script["source"]
    assert source.startswith("def deepCopy(")
    assert source.count("def deepCopy(") == 1
    assert source.index("def __stac_datetime") < source.index("= deepCopy(ctx._source")
    assert source.rindex("__stac_datetime") > source.index("= deepCopy(ctx._source")


def test_array_element_in_path_is_indexed_not_keyed():
    replace = PatchAddReplaceTest.model_validate(
        {"op": "replace", "path": "/properties/l/0/k", "value": 2}
    )

    source = operations_to_script([replace])["source"]

    assert ElasticPath(path="/l/0/k").es_nest == "['l'][0]"
    assert "containsKey('0')" not in source
    assert "ctx._source['properties']['l'][0]['k'] = params.p0;" in source


def test_numeric_member_of_object_is_read_by_string_key():
    copy = PatchMoveCopy.model_validate(
        {"op": "copy", "path": "/properties/q", "from": "/properties/n/1"}
    )
    test = PatchAddReplaceTest.model_validate(
        {"op": "test", "path": "/properties/n/1", "value": "v"}
    )

    copy_source = operations_to_script([copy])["source"]
    test_source = operations_to_script([test])["source"]

    assert "ctx._source['properties']['n']['1']" in copy_source
    assert "ctx._source['properties']['n']['1'] != params.p0" in test_source
