from src.output.Output import Output
import warnings

from polars import Schema, DataFrame


class APIOutput(Output):
    url: str

    def __init__(self, name: str, url: str) -> None:
        super().__init__(name)
        self.url = url

    def describe(self) -> str:
        return f"{self.base_string()} : filepath -> {self.url}"

    def fetch_current_content(self) -> None:
        pass

    def update(self, id, value):
        pass

    def delete(self, id, value):
        pass

    def create(self, id, value):
        pass

    def skip(self, id, value):
        pass

    def persist_changes(
        self,
        row_comparison: DataFrame,
        is_schema_identical: bool,
        new_schema: Schema,
        dry_run: bool,
    ):
        print("Persisting changes...")
        if not is_schema_identical:
            warnings.warn("Schema is different, wiping data")
            # wipe data
        print(row_comparison)
