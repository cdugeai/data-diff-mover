import abc
from polars import DataFrame, Schema, col

from src.Input import Input
from src.RowState import RowState
from src.helpers.compare import (
    dataframe_compare,
    is_schema_identical,
)


class Output:
    current_content: DataFrame
    name: str
    primary_key: str | None
    use_row_index_as_primary_key: bool

    has_fetched: bool

    def __init__(
        self,
        name: str,
        primary_key: str | None,
        use_row_index_as_primary_key: bool,
    ) -> None:  # pragma: no cover
        if primary_key is None and not use_row_index_as_primary_key:
            raise RuntimeError(
                "No primary key provided, please set use_row_index_as_primary_key to True"
            )
        self.current_content = DataFrame()
        self.name = name
        self.primary_key = primary_key
        self.use_row_index_as_primary_key = use_row_index_as_primary_key
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

    def diff(self, input_: Input) -> DataFrame:  # pragma: no cover
        if not input_.has_load:
            raise RuntimeError("Please, load input data first")
        if not self.has_fetched:
            raise RuntimeError("Please, fetch output data first")

        is_identical_schema: bool = is_schema_identical(
            self.current_content.schema, input_.data.schema
        )

        data_compare: DataFrame = dataframe_compare(
            self.current_content,
            self.primary_key,
            input_.data,
            input_.primary_key,
        )
        print(data_compare)
        return data_compare

    def persist_changes(
        self,
        input_: Input,
        dry_run: bool,
    ) -> None:
        new_schema: Schema = input_.data.schema
        is_same_schema: bool = is_schema_identical(
            self.current_content.schema, new_schema
        )
        row_comparison: DataFrame = dataframe_compare(
            self.current_content, self.primary_key, input_.data, input_.primary_key
        )
        print("Persisting changes...")
        if not is_same_schema:
            self.on_schema_changed(self.current_content.schema, new_schema)

        self.hook_on_unchanged(
            row_comparison.filter(col("row_state").eq(RowState.UNCHANGED)),
            is_same_schema,
            new_schema,
            dry_run,
        )
        self.hook_on_deleted(
            row_comparison.filter(col("row_state").eq(RowState.DELETED)),
            is_same_schema,
            new_schema,
            dry_run,
        )
        self.hook_on_created(
            row_comparison.filter(col("row_state").eq(RowState.CREATED)),
            is_same_schema,
            new_schema,
            dry_run,
        )
        self.hook_on_updated(
            row_comparison.filter(col("row_state").eq(RowState.UPDATED)),
            is_same_schema,
            new_schema,
            dry_run,
        )
        self.hook_finally(row_comparison, is_same_schema, new_schema, dry_run)
