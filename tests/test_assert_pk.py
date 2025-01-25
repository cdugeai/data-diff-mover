from src.Comparer import Comparer
import polars as pl


def test_assert_pk_false():
    df_in: pl.DataFrame = pl.from_dicts([
        {"a": 2, "b": 5},
        {"a": 3, "b": 6},
        {"a": 3, "b": 6}
    ])  # fmt: skip

    assert not Comparer.assert_is_primary_key(df_in, "a")


def test_assert_pk_true():
    df_in: pl.DataFrame = pl.from_dicts([
        {"a": 2, "b": 5},
        {"a": 3, "b": 6},
        {"a": 4, "b": 6}
    ])  # fmt: skip

    assert Comparer.assert_is_primary_key(df_in, "a")
