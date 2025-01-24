import abc
from polars import DataFrame, Schema, col

from src.RowState import RowState


class Output:
    current_content: DataFrame
    name: str
    has_fetched: bool

    def __init__(self, name: str) -> None:  # pragma: no cover
        self.current_content = DataFrame()
        self.name = name
        self.has_fetched = False
        pass

    def fetch_current_content(self) -> None:  # pragma: no cover
        self.current_content = self.fetch()
        self.has_fetched = True
        pass

    @abc.abstractmethod
    def fetch(self) -> DataFrame:
        pass  # pragma: no cover

    @abc.abstractmethod
    def describe(self) -> str:
        return ""  # pragma: no cover

    def base_string(self) -> str:
        return f"<{self.__class__.__name__}> {self.name}"  # pragma: no cover

    def __str__(self) -> str:
        return f"{self.base_string()} : {self.current_content.__str__()}"  # pragma: no cover

    @abc.abstractmethod
    def hook_on_deleted(
        self,
        rows_deleted: DataFrame,
        is_schema_identical: bool,
        new_schema: Schema,
        dry_run: bool,
    ) -> None:
        pass  # pragma: no cover

    @abc.abstractmethod
    def hook_on_updated(
        self,
        rows_updated: DataFrame,
        is_schema_identical: bool,
        new_schema: Schema,
        dry_run: bool,
    ) -> None:
        pass  # pragma: no cover

    @abc.abstractmethod
    def hook_on_created(
        self,
        rows_created: DataFrame,
        is_schema_identical: bool,
        new_schema: Schema,
        dry_run: bool,
    ) -> None:
        pass  # pragma: no cover

    @abc.abstractmethod
    def hook_on_unchanged(
        self,
        rows_unchanged: DataFrame,
        is_schema_identical: bool,
        new_schema: Schema,
        dry_run: bool,
    ) -> None:
        pass  # pragma: no cover

    @abc.abstractmethod
    def hook_finally(
        self,
        rows: DataFrame,
        is_schema_identical: bool,
        new_schema: Schema,
        dry_run: bool,
    ) -> None:
        pass  # pragma: no cover

    @abc.abstractmethod
    def on_schema_changed(self, current_schema: Schema, new_schema: Schema) -> None:
        pass  # pragma: no cover

    def persist_changes(
        self,
        row_comparison: DataFrame,
        is_schema_identical: bool,
        new_schema: Schema,
        dry_run: bool,
    ) -> None:
        print("Persisting changes...")
        if not is_schema_identical:
            self.on_schema_changed(self.current_content.schema, new_schema)

        self.hook_on_unchanged(
            row_comparison.filter(col("row_state").eq(RowState.UNCHANGED)),
            is_schema_identical,
            new_schema,
            dry_run,
        )
        self.hook_on_deleted(
            row_comparison.filter(col("row_state").eq(RowState.DELETED)),
            is_schema_identical,
            new_schema,
            dry_run,
        )
        self.hook_on_created(
            row_comparison.filter(col("row_state").eq(RowState.CREATED)),
            is_schema_identical,
            new_schema,
            dry_run,
        )
        self.hook_on_updated(
            row_comparison.filter(col("row_state").eq(RowState.UPDATED)),
            is_schema_identical,
            new_schema,
            dry_run,
        )
        self.hook_finally(row_comparison, is_schema_identical, new_schema, dry_run)
