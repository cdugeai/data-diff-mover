from src.Comparer import Comparer
import polars as pl
from polars.testing import assert_frame_equal

from src.RowState import RowState


def test_compare_delete() -> None:
    df_current = pl.from_dicts([{"a": 1, "b": 4}, {"a": 2, "b": 5}, {"a": 3, "b": 6}])
    df_new = pl.from_dicts([{"a": 2, "b": 5}])
    comparison = Comparer.dataframe_compare(df_current, df_new)
    expected_comparison = pl.from_dicts([
        {"idx_current": 0, "idx_new": 0,"row_state": RowState.UPDATED.value,"a":2,"b":5},
        {"idx_current": 1, "idx_new": None, "row_state": RowState.DELETED.value, "a": None, "b": None},
        {"idx_current": 2, "idx_new": None, "row_state": RowState.DELETED.value, "a": None, "b": None},
    ],
    schema={"idx_current": pl.UInt32, "idx_new": pl.UInt32,"row_state": str,"a": int, "b":int})  # fmt: skip
    assert_frame_equal(comparison, expected_comparison)


def test_compare_create() -> None:
    df_current = pl.from_dicts([{"a": 1, "b": 4}])
    df_new = pl.from_dicts([{"a": 2, "b": 5}, {"a": 3, "b": 6}, {"a": 3, "b": 6}])
    comparison = Comparer.dataframe_compare(df_current, df_new)
    expected_comparison = pl.from_dicts([
        {"idx_current": 0, "idx_new": 0,"row_state": RowState.UPDATED.value,"a": 2, "b":5},
        {"idx_current": None, "idx_new": 1, "row_state": RowState.CREATED.value, "a": 3, "b": 6},
        {"idx_current": None, "idx_new": 2, "row_state": RowState.CREATED.value, "a": 3, "b": 6},
    ],
    schema={"idx_current": pl.UInt32, "idx_new": pl.UInt32,"row_state": str,"a": int, "b":int})  # fmt: skip
    assert_frame_equal(comparison, expected_comparison)


def test_compare_add_column() -> None:
    df_current = pl.from_dicts([{"a": 4}, {"a": 90}])
    df_new = pl.from_dicts([{"a": 2, "b": 5}, {"a": 3, "b": 6}, {"a": 3, "b": 6}])
    comparison = Comparer.dataframe_compare(df_current, df_new)
    expected_comparison = pl.from_dicts([
        {"idx_current": 0, "idx_new": 0,"row_state": RowState.UPDATED.value,"a": 2, "b":5},
        {"idx_current": 1, "idx_new": 1, "row_state": RowState.UPDATED.value, "a": 3, "b": 6},
        {"idx_current": None, "idx_new": 2, "row_state": RowState.CREATED.value, "a": 3, "b": 6},
    ],
    schema={"idx_current": pl.UInt32, "idx_new": pl.UInt32,"row_state": str,"a": int, "b":int})  # fmt: skip
    assert_frame_equal(comparison, expected_comparison)


def test_compare_drop_column() -> None:
    df_current = pl.from_dicts([{"a": 2, "b": 5}, {"a": 3, "b": 6}, {"a": 3, "b": 6}])
    df_new = pl.from_dicts([{"a": 4}, {"a": 90}])
    comparison = Comparer.dataframe_compare(df_current, df_new)
    expected_comparison = pl.from_dicts([
        {"idx_current": 0, "idx_new": 0,"row_state": RowState.UPDATED.value,"a": 4},
        {"idx_current": 1, "idx_new": 1, "row_state": RowState.UPDATED.value, "a": 90},
        {"idx_current": 2, "idx_new": None, "row_state": RowState.DELETED.value, "a": None},
    ],
    schema={"idx_current": pl.UInt32, "idx_new": pl.UInt32,"row_state": str,"a": int})  # fmt: skip
    assert_frame_equal(comparison, expected_comparison)


def test_compare_unchanged() -> None:
    df_current = pl.from_dicts([{"a": 2, "b": 5}, {"a": 3, "b": 6}, {"a": 3, "b": 6}])
    df_new = pl.from_dicts([{"a": 6, "b": 4}, {"a": 3, "b": 6}])
    comparison = Comparer.dataframe_compare(df_current, df_new)
    expected_comparison = pl.from_dicts([
            {"idx_current": 0, "idx_new": 0, "row_state": RowState.UPDATED.value, "a": 6, "b": 4},
            {"idx_current": 1, "idx_new": 1, "row_state": RowState.UNCHANGED.value, "a": 3, "b": 6},
            {"idx_current": 2, "idx_new": None, "row_state": RowState.DELETED.value, "a": None, "b": None},
        ],
            schema={"idx_current": pl.UInt32, "idx_new": pl.UInt32, "row_state": str, "a": int, "b": int})  # fmt: skip
    assert_frame_equal(comparison, expected_comparison)


def test_compare_input_empty() -> None:
    df_current = pl.DataFrame(schema={"a": int, "b": int})
    df_new = pl.from_dicts([{"a": 2, "b": 5}, {"a": 3, "b": 6}, {"a": 3, "b": 6}])
    comparison = Comparer.dataframe_compare(df_current, df_new)
    expected_comparison = pl.from_dicts([
        {"idx_current": None, "idx_new": 0, "row_state": RowState.CREATED.value, "a": 2, "b": 5},
        {"idx_current": None, "idx_new": 1, "row_state": RowState.CREATED.value, "a": 3, "b": 6},
        {"idx_current": None, "idx_new": 2, "row_state": RowState.CREATED.value, "a": 3, "b": 6},
    ],
        schema={"idx_current": pl.UInt32, "idx_new": pl.UInt32, "row_state": str, "a": int, "b": int})  # fmt: skip
    assert_frame_equal(comparison, expected_comparison)


def test_compare_output_empty() -> None:
    df_current = pl.from_dicts([{"a": 2, "b": 5}, {"a": 3, "b": 6}, {"a": 3, "b": 6}])
    df_new = pl.DataFrame(schema={"a": int, "b": int})
    comparison = Comparer.dataframe_compare(df_current, df_new)
    expected_comparison = pl.from_dicts([
        {"idx_current": 0, "idx_new": None, "row_state": RowState.DELETED.value, "a": None, "b": None},
        {"idx_current": 1, "idx_new": None, "row_state": RowState.DELETED.value, "a": None, "b": None},
        {"idx_current": 2, "idx_new": None, "row_state": RowState.DELETED.value, "a": None, "b": None},
    ],
        schema={"idx_current": pl.UInt32, "idx_new": pl.UInt32, "row_state": str, "a": int, "b": int})  # fmt: skip
    assert_frame_equal(comparison, expected_comparison)


def test_compare_input_empty_schema_changed() -> None:
    df_current = pl.DataFrame(schema={"a": str})
    df_new = pl.from_dicts([{"a": 2, "b": 5}, {"a": 3, "b": 6}, {"a": 3, "b": 6}])
    comparison = Comparer.dataframe_compare(df_current, df_new)
    expected_comparison = pl.from_dicts([
        {"idx_current": None, "idx_new": 0, "row_state": RowState.CREATED.value, "a": 2, "b": 5},
        {"idx_current": None, "idx_new": 1, "row_state": RowState.CREATED.value, "a": 3, "b": 6},
        {"idx_current": None, "idx_new": 2, "row_state": RowState.CREATED.value, "a": 3, "b": 6},
    ],
        schema={"idx_current": pl.UInt32, "idx_new": pl.UInt32, "row_state": str, "a": int, "b": int})  # fmt: skip
    assert_frame_equal(comparison, expected_comparison)


def test_compare_output_empty_schema_changed() -> None:
    df_current = pl.from_dicts([{"a": 2, "b": 5}, {"a": 3, "b": 6}, {"a": 3, "b": 6}])
    df_new = pl.DataFrame(schema={"a": int})
    comparison = Comparer.dataframe_compare(df_current, df_new)
    expected_comparison = pl.from_dicts([
        {"idx_current": 0, "idx_new": None, "row_state": RowState.DELETED.value, "a": None,},
        {"idx_current": 1, "idx_new": None, "row_state": RowState.DELETED.value, "a": None},
        {"idx_current": 2, "idx_new": None, "row_state": RowState.DELETED.value, "a": None},
    ],
        schema={"idx_current": pl.UInt32, "idx_new": pl.UInt32, "row_state": str, "a": int})  # fmt: skip
    assert_frame_equal(comparison, expected_comparison)
