from src.helpers.compare import assert_is_primary_key
import polars as pl
from pytest import raises

from src.exceptions import ColumnNameIsNotPK


def test_assert_pk_false() -> None:
    with raises(ColumnNameIsNotPK):
        df_in: pl.DataFrame = pl.from_dicts([
            {"a": 2, "b": 5},
            {"a": 3, "b": 6},
            {"a": 3, "b": 6}
        ])  # fmt: skip

        assert not assert_is_primary_key(df_in, "a")


def test_assert_pk_true() -> None:
    df_in: pl.DataFrame = pl.from_dicts([
        {"a": 2, "b": 5},
        {"a": 3, "b": 6},
        {"a": 4, "b": 6}
    ])  # fmt: skip

    assert assert_is_primary_key(df_in, "a")
