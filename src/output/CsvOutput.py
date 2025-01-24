from src.Comparer import RowState
from src.output.Output import Output
import warnings

from polars import read_csv, DataFrame, col, Schema


class CsvOutput(Output):
    path: str

    def __init__(self, name: str, path: str) -> None:
        super().__init__(name)
        self.path = path

    def describe(self) -> str:
        return f"{self.base_string()} : filepath -> {self.path}"

    def fetch_current_content(self) -> None:
        try:
            self.current_content = read_csv(self.path)
            self.has_fetched = True
        except FileNotFoundError:
            warnings.warn(
                self.base_string()
                + ": No such file found for "
                + self.path
                + ". Using empty dataframe as fetched data."
            )
            self.current_content = DataFrame()
            self.has_fetched = True

    def persist_changes(
        self,
        row_comparison: DataFrame,
        is_schema_identical: bool,
        new_schema: Schema,
        dry_run: bool,
    ):
        print("Persisting changes...")
        if not is_schema_identical:
            # wipe data not needed on CSV output
            pass

        print("Changes preview:")
        print(row_comparison.group_by(col("row_state")).count())

        new_df = row_comparison.filter(col("row_state").ne(RowState.DELETED)).select(
            new_schema
        )

        if not dry_run:
            new_df.write_csv(self.path)
