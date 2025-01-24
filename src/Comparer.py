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
        return self.dataframe_compare(self.output_.current_content, self.input_.data)

    def persist_compare(self, dry_run: bool) -> None:  # pragma: no cover
        self.output_.persist_changes(
            self.data_compare,
            self.is_identical_schema,
            self.input_.data.schema,
            dry_run,
        )

    @staticmethod
    def dataframe_compare(df_base: DataFrame, df_new: DataFrame) -> DataFrame:
        current_rows_hash: Series = df_base.hash_rows(seed_1=1)
        current_content_with_id = df_base.with_row_index(name="idx_current")
        data_current: DataFrame = current_content_with_id.with_columns(
            current_rows_hash.alias("hash_row_current")
        )

        new_content_with_id = df_new.with_row_index(name="idx_new")
        new_rows_hash: Series = df_new.hash_rows(seed_1=1)
        data_new: DataFrame = new_content_with_id.with_columns(
            new_rows_hash.alias("hash_row_new")
        )

        diff_values_df = (
            data_current.select(
                col("idx_current"),
                col("hash_row_current"),
            )
            .join(
                data_new.select(col("idx_new"), col("hash_row_new")),
                left_on="idx_current",
                right_on="idx_new",
                how="full",
            )
            .with_columns(
                col("hash_row_new").eq(col("hash_row_current")).alias("data_identical"),
            )
            .with_columns(
                when(col("data_identical"))
                .then(lit(RowState.UNCHANGED))
                .when(col("idx_current").is_null())
                .then(lit(RowState.CREATED))
                .when(col("data_identical").not_())
                .then(lit(RowState.UPDATED))
                .when(col("idx_new").is_null())
                .then(lit(RowState.DELETED))
                .alias("row_state")
            )
        )

        changes_rows = diff_values_df.select(
            col("idx_current"), col("idx_new"), col("row_state")
        ).join(
            new_content_with_id,
            left_on="idx_new",
            right_on="idx_new",
            how="left",
        )

        return changes_rows
