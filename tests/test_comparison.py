from src.helpers.compare import dataframe_compare
import polars as pl
from polars.testing import assert_frame_equal
from pytest import raises
from src.RowState import RowState
from src.exceptions import ColumnNameIsNotPK


def test_compare_delete() -> None:
    df_current = pl.from_dicts([{"a": 1, "b": 4}, {"a": 2, "b": 5}, {"a": 3, "b": 6}])
    df_new = pl.from_dicts([{"a": 2, "b": 5}])
    comparison = dataframe_compare(df_current, None, df_new, None)
    expected_comparison = pl.from_dicts([
        {"pk_current": 0, "pk_new": 0,"row_state": RowState.UPDATED.value,"a":2,"b":5},
        {"pk_current": 1, "pk_new": None, "row_state": RowState.DELETED.value, "a": None, "b": None},
        {"pk_current": 2, "pk_new": None, "row_state": RowState.DELETED.value, "a": None, "b": None},
    ],
    schema={"pk_current": pl.UInt32, "pk_new": pl.UInt32,"row_state": str,"a": int, "b":int})  # fmt: skip
    assert_frame_equal(comparison, expected_comparison)


def test_compare_create() -> None:
    df_current = pl.from_dicts([{"a": 1, "b": 4}])
    df_new = pl.from_dicts([{"a": 2, "b": 5}, {"a": 3, "b": 6}, {"a": 3, "b": 6}])
    comparison = dataframe_compare(df_current, None, df_new, None)
    expected_comparison = pl.from_dicts([
        {"pk_current": 0, "pk_new": 0,"row_state": RowState.UPDATED.value,"a": 2, "b":5},
        {"pk_current": None, "pk_new": 1, "row_state": RowState.CREATED.value, "a": 3, "b": 6},
        {"pk_current": None, "pk_new": 2, "row_state": RowState.CREATED.value, "a": 3, "b": 6},
    ],
    schema={"pk_current": pl.UInt32, "pk_new": pl.UInt32,"row_state": str,"a": int, "b":int})  # fmt: skip
    assert_frame_equal(comparison, expected_comparison)


def test_compare_add_column() -> None:
    df_current = pl.from_dicts([{"a": 4}, {"a": 90}])
    df_new = pl.from_dicts([{"a": 2, "b": 5}, {"a": 3, "b": 6}, {"a": 3, "b": 6}])
    comparison = dataframe_compare(df_current, None, df_new, None)
    expected_comparison = pl.from_dicts([
        {"pk_current": 0, "pk_new": 0,"row_state": RowState.UPDATED.value,"a": 2, "b":5},
        {"pk_current": 1, "pk_new": 1, "row_state": RowState.UPDATED.value, "a": 3, "b": 6},
        {"pk_current": None, "pk_new": 2, "row_state": RowState.CREATED.value, "a": 3, "b": 6},
    ],
    schema={"pk_current": pl.UInt32, "pk_new": pl.UInt32,"row_state": str,"a": int, "b":int})  # fmt: skip
    assert_frame_equal(comparison, expected_comparison)


def test_compare_drop_column() -> None:
    df_current = pl.from_dicts([{"a": 2, "b": 5}, {"a": 3, "b": 6}, {"a": 3, "b": 6}])
    df_new = pl.from_dicts([{"a": 4}, {"a": 90}])
    comparison = dataframe_compare(df_current, None, df_new, None)
    expected_comparison = pl.from_dicts([
        {"pk_current": 0, "pk_new": 0,"row_state": RowState.UPDATED.value,"a": 4},
        {"pk_current": 1, "pk_new": 1, "row_state": RowState.UPDATED.value, "a": 90},
        {"pk_current": 2, "pk_new": None, "row_state": RowState.DELETED.value, "a": None},
    ],
    schema={"pk_current": pl.UInt32, "pk_new": pl.UInt32,"row_state": str,"a": int})  # fmt: skip
    assert_frame_equal(comparison, expected_comparison)


def test_compare_unchanged() -> None:
    df_current = pl.from_dicts([{"a": 2, "b": 5}, {"a": 3, "b": 6}, {"a": 3, "b": 6}])
    df_new = pl.from_dicts([{"a": 6, "b": 4}, {"a": 3, "b": 6}])
    comparison = dataframe_compare(df_current, None, df_new, None)
    expected_comparison = pl.from_dicts([
            {"pk_current": 0, "pk_new": 0, "row_state": RowState.UPDATED.value, "a": 6, "b": 4},
            {"pk_current": 1, "pk_new": 1, "row_state": RowState.UNCHANGED.value, "a": 3, "b": 6},
            {"pk_current": 2, "pk_new": None, "row_state": RowState.DELETED.value, "a": None, "b": None},
        ],
            schema={"pk_current": pl.UInt32, "pk_new": pl.UInt32, "row_state": str, "a": int, "b": int})  # fmt: skip
    assert_frame_equal(comparison, expected_comparison)


def test_compare_input_empty() -> None:
    df_current = pl.DataFrame(schema={"a": int, "b": int})
    df_new = pl.from_dicts([{"a": 2, "b": 5}, {"a": 3, "b": 6}, {"a": 3, "b": 6}])
    comparison = dataframe_compare(df_current, None, df_new, None)
    expected_comparison = pl.from_dicts([
        {"pk_current": None, "pk_new": 0, "row_state": RowState.CREATED.value, "a": 2, "b": 5},
        {"pk_current": None, "pk_new": 1, "row_state": RowState.CREATED.value, "a": 3, "b": 6},
        {"pk_current": None, "pk_new": 2, "row_state": RowState.CREATED.value, "a": 3, "b": 6},
    ],
        schema={"pk_current": pl.UInt32, "pk_new": pl.UInt32, "row_state": str, "a": int, "b": int})  # fmt: skip
    assert_frame_equal(comparison, expected_comparison)


def test_compare_output_empty() -> None:
    df_current = pl.from_dicts([{"a": 2, "b": 5}, {"a": 3, "b": 6}, {"a": 3, "b": 6}])
    df_new = pl.DataFrame(schema={"a": int, "b": int})
    comparison = dataframe_compare(df_current, None, df_new, None)
    expected_comparison = pl.from_dicts([
        {"pk_current": 0, "pk_new": None, "row_state": RowState.DELETED.value, "a": None, "b": None},
        {"pk_current": 1, "pk_new": None, "row_state": RowState.DELETED.value, "a": None, "b": None},
        {"pk_current": 2, "pk_new": None, "row_state": RowState.DELETED.value, "a": None, "b": None},
    ],
        schema={"pk_current": pl.UInt32, "pk_new": pl.UInt32, "row_state": str, "a": int, "b": int})  # fmt: skip
    assert_frame_equal(comparison, expected_comparison)


def test_compare_input_empty_schema_changed() -> None:
    df_current = pl.DataFrame(schema={"a": str})
    df_new = pl.from_dicts([{"a": 2, "b": 5}, {"a": 3, "b": 6}, {"a": 3, "b": 6}])
    comparison = dataframe_compare(df_current, None, df_new, None)
    expected_comparison = pl.from_dicts([
        {"pk_current": None, "pk_new": 0, "row_state": RowState.CREATED.value, "a": 2, "b": 5},
        {"pk_current": None, "pk_new": 1, "row_state": RowState.CREATED.value, "a": 3, "b": 6},
        {"pk_current": None, "pk_new": 2, "row_state": RowState.CREATED.value, "a": 3, "b": 6},
    ],
        schema={"pk_current": pl.UInt32, "pk_new": pl.UInt32, "row_state": str, "a": int, "b": int})  # fmt: skip
    assert_frame_equal(comparison, expected_comparison)


def test_compare_output_empty_schema_changed() -> None:
    df_current = pl.from_dicts([{"a": 2, "b": 5}, {"a": 3, "b": 6}, {"a": 3, "b": 6}])
    df_new = pl.DataFrame(schema={"a": int})
    comparison = dataframe_compare(df_current, None, df_new, None)
    expected_comparison = pl.from_dicts([
        {"pk_current": 0, "pk_new": None, "row_state": RowState.DELETED.value, "a": None,},
        {"pk_current": 1, "pk_new": None, "row_state": RowState.DELETED.value, "a": None},
        {"pk_current": 2, "pk_new": None, "row_state": RowState.DELETED.value, "a": None},
    ],
        schema={"pk_current": pl.UInt32, "pk_new": pl.UInt32, "row_state": str, "a": int})  # fmt: skip
    assert_frame_equal(comparison, expected_comparison)


def test_compare_delete_with_pk() -> None:
    df_current = pl.from_dicts([{"a": 1, "b": 4}, {"a": 2, "b": 5}, {"a": 3, "b": 6}])
    df_new = pl.from_dicts([{"a": 2, "b": 6}])
    comparison = dataframe_compare(df_current, "a", df_new, "a")
    expected_comparison = pl.from_dicts([
        {"pk_current": 1, "pk_new": None,"row_state": RowState.DELETED.value,"a":None,"b":None},
        {"pk_current": 2, "pk_new": 2, "row_state": RowState.UPDATED.value, "a": 2, "b": 6},
        {"pk_current": 3, "pk_new": None, "row_state": RowState.DELETED.value, "a": None, "b": None},
    ],
    schema={"pk_current": int, "pk_new": int,"row_state": str,"a": int, "b":int})  # fmt: skip
    assert_frame_equal(comparison, expected_comparison)


def test_compare_unchanged_with_pk() -> None:
    df_current = pl.from_dicts([{"a": 1, "b": 4}, {"a": 2, "b": 5}, {"a": 3, "b": 6}])
    df_new = pl.from_dicts([{"a": 2, "b": 5}])
    comparison = dataframe_compare(df_current, "a", df_new, "a")
    expected_comparison = pl.from_dicts([
        {"pk_current": 1, "pk_new": None,"row_state": RowState.DELETED.value,"a":None,"b":None},
        {"pk_current": 2, "pk_new": 2, "row_state": RowState.UNCHANGED.value, "a": 2, "b": 5},
        {"pk_current": 3, "pk_new": None, "row_state": RowState.DELETED.value, "a": None, "b": None},
    ],
    schema={"pk_current": int, "pk_new": int,"row_state": str,"a": int, "b":int})  # fmt: skip
    assert_frame_equal(comparison, expected_comparison)


def test_compare_create_with_pk() -> None:
    df_current = pl.from_dicts([{"a": 1, "b": 4}])
    df_new = pl.from_dicts([{"a": 2, "b": 5}, {"a": 3, "b": 6}, {"a": 1, "b": 6}])
    comparison = dataframe_compare(df_current, "a", df_new, "a")
    expected_comparison = pl.from_dicts([
        {"pk_current": None, "pk_new": 2, "row_state": RowState.CREATED.value, "a": 2, "b": 5},
        {"pk_current": None, "pk_new": 3, "row_state": RowState.CREATED.value, "a": 3, "b": 6},
        {"pk_current": 1, "pk_new": 1, "row_state": RowState.UPDATED.value, "a": 1, "b": 6},
    ],
    schema={"pk_current": int, "pk_new": int,"row_state": str,"a": int, "b":int})  # fmt: skip
    assert_frame_equal(comparison, expected_comparison)


def test_compare_wrong_pk() -> None:
    with raises(ColumnNameIsNotPK):
        df_current = pl.from_dicts([{"a": 1, "b": 4}])
        df_new = pl.from_dicts([{"a": 2, "b": 5}, {"a": 2, "b": 6}, {"a": 1, "b": 6}])
        comparison = dataframe_compare(df_current, "a", df_new, "a")
        expected_comparison = pl.from_dicts([
            {"pk_current": None, "pk_new": 2, "row_state": RowState.CREATED.value, "a": 2, "b": 5},
            {"pk_current": None, "pk_new": 3, "row_state": RowState.CREATED.value, "a": 3, "b": 6},
            {"pk_current": 1, "pk_new": 1, "row_state": RowState.UPDATED.value, "a": 1, "b": 6},
        ],
        schema={"pk_current": int, "pk_new": int,"row_state": str,"a": int, "b":int})  # fmt: skip
        print(comparison)
        print(expected_comparison)
        assert_frame_equal(comparison, expected_comparison)


def test_compare_create_with_pk_different_names() -> None:
    # pk is "a"
    df_current = pl.from_dicts([{"a": 1, "b": 5}])
    # pk is "d"
    df_new = pl.from_dicts([{"c": 2, "d": 5}, {"c": 3, "d": 6}, {"c": 10, "d": 1}])
    comparison = dataframe_compare(df_current, "a", df_new, "d")
    expected_comparison = pl.from_dicts([
        {"pk_current": None, "pk_new": 5, "row_state": RowState.CREATED.value, "c": 2, "d": 5},
        {"pk_current": None, "pk_new": 6, "row_state": RowState.CREATED.value, "c": 3, "d": 6},
        {"pk_current": 1, "pk_new": 1, "row_state": RowState.UPDATED.value, "c": 10, "d": 1},
    ],
    schema={"pk_current": int, "pk_new": int,"row_state": str,"c": int, "d":int})  # fmt: skip
    assert_frame_equal(comparison, expected_comparison)


def test_compare_create_with_pk_swapped_names() -> None:
    # pk is "a"
    df_current = pl.from_dicts([{"a": 1, "b": 5}])
    # pk is "b"
    df_new = pl.from_dicts([{"a": 2, "b": 5}, {"a": 3, "b": 6}, {"a": 10, "b": 1}])
    comparison = dataframe_compare(df_current, "a", df_new, "b")
    expected_comparison = pl.from_dicts([
        {"pk_current": None, "pk_new": 5, "row_state": RowState.CREATED.value, "a": 2, "b": 5},
        {"pk_current": None, "pk_new": 6, "row_state": RowState.CREATED.value, "a": 3, "b": 6},
        {"pk_current": 1, "pk_new": 1, "row_state": RowState.UPDATED.value, "a": 10, "b": 1},
    ],
    schema={"pk_current": int, "pk_new": int,"row_state": str,"a": int, "b":int})  # fmt: skip
    assert_frame_equal(comparison, expected_comparison)
