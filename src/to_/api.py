import abc
from src.Output import Output

from polars import Schema, DataFrame


class API(Output):
    base_url: str
    headers: dict[str, str]

    def __init__(
        self,
        name: str,
        base_url: str,
        primary_key: str | None = None,
        use_row_index_as_primary_key: bool = False,
    ) -> None:
        super().__init__(name, primary_key, use_row_index_as_primary_key)
        self.base_url = base_url
        self.headers = {}

    def describe(self) -> str:
        return f"{self.base_string()} : filepath -> {self.base_url}"

    @abc.abstractmethod
    def fetch_current_content(self) -> None:
        pass

    @abc.abstractmethod
    def hook_on_deleted(
        self,
        rows_deleted: DataFrame,
        is_schema_identical: bool,
        new_schema: Schema,
        dry_run: bool,
    ) -> None:
        pass

    @abc.abstractmethod
    def hook_on_updated(
        self,
        rows_updated: DataFrame,
        is_schema_identical: bool,
        new_schema: Schema,
        dry_run: bool,
    ) -> None:
        pass

    @abc.abstractmethod
    def hook_on_created(
        self,
        rows_created: DataFrame,
        is_schema_identical: bool,
        new_schema: Schema,
        dry_run: bool,
    ) -> None:
        pass

    @abc.abstractmethod
    def hook_on_unchanged(
        self,
        rows_unchanged: DataFrame,
        is_schema_identical: bool,
        new_schema: Schema,
        dry_run: bool,
    ) -> None:
        pass

    @abc.abstractmethod
    def hook_finally(
        self,
        rows: DataFrame,
        is_schema_identical: bool,
        new_schema: Schema,
        dry_run: bool,
    ) -> None:
        pass

    @abc.abstractmethod
    def on_schema_changed(self, current_schema: Schema, new_schema: Schema) -> None:
        pass
