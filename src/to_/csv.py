from src.RowState import RowState
from src.Output import Output
import warnings

from polars import read_csv, DataFrame, col, Schema


class CsvFile(Output):
    def hook_on_deleted(
        self,
        rows_deleted: DataFrame,
        is_schema_identical: bool,
        new_schema: Schema,
        dry_run: bool,
    ) -> None:
        pass

    def hook_on_unchanged(
        self,
        rows_unchanged: DataFrame,
        is_schema_identical: bool,
        new_schema: Schema,
        dry_run: bool,
    ) -> None:
        pass

    def hook_on_updated(
        self,
        rows_updated: DataFrame,
        is_schema_identical: bool,
        new_schema: Schema,
        dry_run: bool,
    ) -> None:
        pass

    def hook_on_created(
        self,
        rows_created: DataFrame,
        is_schema_identical: bool,
        new_schema: Schema,
        dry_run: bool,
    ) -> None:
        pass

    def hook_finally(
        self,
        rows: DataFrame,
        is_schema_identical: bool,
        new_schema: Schema,
        dry_run: bool,
    ) -> None:
        # Select all rows but DELETED
        new_df = rows.filter(col("row_state").ne(RowState.DELETED)).select(new_schema)
        if not dry_run:
            # Persist DF to file
            new_df.write_csv(self.path)

    def on_schema_changed(self, current_schema: Schema, new_schema: Schema) -> None:
        warnings.warn("Warning: Schema of input CSV has changed")
        pass

    path: str

    def __init__(
        self,
        name: str,
        path: str,
        primary_key: str | None = None,
        use_row_index_as_primary_key: bool = False,
    ) -> None:
        super().__init__(
            name,
            primary_key,
            use_row_index_as_primary_key,
        )
        self.path = path

    def describe(self) -> str:  # pragma: no cover
        return f"{self.base_string()} : filepath -> {self.path}"

    def fetch(self) -> DataFrame:
        try:
            return read_csv(self.path)
        except FileNotFoundError:
            warnings.warn(
                self.base_string()
                + ": No such file found for "
                + self.path
                + ". Using empty dataframe as fetched data."
            )
            return DataFrame()
