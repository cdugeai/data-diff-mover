import polars as pl

from src.helpers.compare import is_schema_identical


def test_schema_identical() -> None:
    df1 = pl.from_dicts([{"a": 1, "b": 4}])
    df2 = pl.from_dicts([{"a": 6, "b": 7}])

    assert is_schema_identical(df1.schema, df2.schema)


def test_schema_different() -> None:
    df1 = pl.from_dicts([{"a": 1, "b": "some text", "c": "another column"}])
    df2 = pl.from_dicts([{"a": 6, "b": 7}])

    assert not is_schema_identical(df1.schema, df2.schema)
