from src.RowState import RowState
from src.exceptions import ColumnNameIsNotPK
from polars import DataFrame, Schema, Series, col, lit, when


def is_schema_identical(schema1: Schema, schema2: Schema) -> bool:
    return schema1.__eq__(schema2)


def assert_is_primary_key(df: DataFrame, pk_colname: str) -> bool:
    """
    Returns if pk_colname is actually a primary key
    Check for the given key if multiples rows are found
    :param df: input Dataframe
    :param pk_colname: Name of the column to check for PK
    :return:

    :raises ColumnNameIsNotPK: if the column is not PK
    """

    n_pk_with_multiple_rows = (
        df.select(col(pk_colname))
        .group_by(col(pk_colname))
        .len(name="occurences_pk")
        .filter(col("occurences_pk").gt(1))
        .height
    )

    if n_pk_with_multiple_rows > 0:
        raise ColumnNameIsNotPK(column_name=pk_colname)
    else:
        return True


def dataframe_compare(
    df_base: DataFrame,
    pk_colname_base: str | None,
    df_new: DataFrame,
    pk_colname_new: str | None,
) -> DataFrame:
    """

    :param df_base:
    :param pk_colname_base:
    :param df_new:
    :param pk_colname_new:
    :return:

    :raises ColumnNameIsNotPK: if the column is not PK
    """
    current_rows_hash: Series = df_base.hash_rows()

    # No PK column name provided
    if pk_colname_base is None:
        # PK will be row index
        current_content_with_pk = df_base.with_row_index(name="pk_current")
    else:
        # Assert is PK or raise Error
        assert_is_primary_key(df_base, pk_colname_base)
        # PK is specified column
        current_content_with_pk = df_base.with_columns(
            col(pk_colname_base).alias("pk_current")
        )

    data_current: DataFrame = current_content_with_pk.with_columns(
        current_rows_hash.alias("hash_row_current")
    )

    # No PK column name provided
    if pk_colname_new is None:
        # PK will be row index
        new_content_with_pk = df_new.with_row_index(name="pk_new")
    else:
        # Assert is PK or raise Error
        assert_is_primary_key(df_new, pk_colname_new)
        # PK is specified column
        new_content_with_pk = df_new.with_columns(col(pk_colname_new).alias("pk_new"))

    new_rows_hash: Series = df_new.hash_rows()
    data_new: DataFrame = new_content_with_pk.with_columns(
        new_rows_hash.alias("hash_row_new")
    )

    diff_values_df = (
        data_current.select(
            col("pk_current"),
            col("hash_row_current"),
        )
        .join(
            data_new.select(col("pk_new"), col("hash_row_new")),
            left_on="pk_current",
            right_on="pk_new",
            how="full",
        )
        .with_columns(
            col("hash_row_new").eq(col("hash_row_current")).alias("data_identical"),
        )
        .with_columns(
            when(col("data_identical"))
            .then(lit(RowState.UNCHANGED))
            .when(col("pk_current").is_null())
            .then(lit(RowState.CREATED))
            .when(col("data_identical").not_())
            .then(lit(RowState.UPDATED))
            .when(col("pk_new").is_null())
            .then(lit(RowState.DELETED))
            .alias("row_state")
        )
    )

    changes_rows = diff_values_df.select(
        col("pk_current"), col("pk_new"), col("row_state")
    ).join(
        new_content_with_pk,
        left_on="pk_new",
        right_on="pk_new",
        how="left",
    )

    return changes_rows
