from src.RowState import RowState
from src.input.Input import Input
from src.output.Output import Output

from polars import DataFrame, Schema, Series, col, lit, when


class Comparer:
    input_: Input
    output_: Output

    is_identical_schema: bool
    data_compare: DataFrame

    def __init__(self, input_: Input, output_: Output) -> None:  # pragma: no cover
        self.input_ = input_
        self.output_ = output_
        self.data_compare = DataFrame()

    def compare(self) -> None:  # pragma: no cover
        if not self.input_.has_load:
            raise RuntimeError("Please, load input data first")
        if not self.output_.has_fetched:
            raise RuntimeError("Please, fetch output data first")

        self.is_identical_schema = self.compare_schema()
        self.data_compare = self.compare_data()

    def __str__(self) -> str:  # pragma: no cover
        return f"Comparer: {self.input_.base_string()} vs {self.output_.base_string()}"

    def compare_schema(self) -> bool:  # pragma: no cover
        schema_current: Schema = self.output_.current_content.collect_schema()
        schema_new: Schema = self.input_.data.collect_schema()

        schema_are_identical: bool = self.compare_schema_static(
            schema_current, schema_new
        )
        if schema_are_identical:
            print("Les schemas sont identiques")
        else:
            print("Les schemas sont différents")
            print("old schema", schema_current)
            print("new schema", schema_new)

        return schema_are_identical

    @staticmethod
    def compare_schema_static(schema1: Schema, schema2: Schema) -> bool:
        return schema1.__eq__(schema2)

    def compare_data(self) -> DataFrame:  # pragma: no cover
        return self.dataframe_compare(
            self.output_.current_content,
            self.output_.primary_key,
            self.input_.data,
            self.input_.primary_key,
        )

    def persist_compare(self, dry_run: bool) -> None:  # pragma: no cover
        self.output_.persist_changes(
            self.data_compare,
            self.is_identical_schema,
            self.input_.data.schema,
            dry_run,
        )

    @classmethod
    def dataframe_compare(
        cls,
        df_base: DataFrame,
        pk_colname_base: str | None,
        df_new: DataFrame,
        pk_colname_new: str | None,
    ) -> DataFrame:
        current_rows_hash: Series = df_base.hash_rows()

        # No PK column name provided
        if pk_colname_base is None:
            # PK will be row index
            current_content_with_pk = df_base.with_row_index(name="pk_current")
        else:
            # Assert is PK
            if not cls.assert_is_primary_key(df_base, pk_colname_base):
                raise RuntimeError(pk_colname_base + " is not a PK for this dataframe")
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
            # Assert is PK
            if not cls.assert_is_primary_key(df_new, pk_colname_new):
                raise RuntimeError(pk_colname_new + " is not a PK for this dataframe")
            # PK is specified column
            new_content_with_pk = df_new.with_columns(
                col(pk_colname_new).alias("pk_new")
            )

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

    @staticmethod
    def assert_is_primary_key(df: DataFrame, pk_colname: str) -> bool:
        """
        Returns if pk_colname is actually a primary key
        Check for the given key if multiples rows are found
        :param df: input Dataframe
        :param pk_colname: Name of the column to check for PK
        :return:
        """
        return (
            df.select(col(pk_colname))
            .group_by(col(pk_colname))
            .len(name="occurences_pk")
            .filter(col("occurences_pk").gt(1))
            .height
            == 0
        )
