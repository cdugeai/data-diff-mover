import abc
from polars import DataFrame, Schema


class Output:
    current_content: DataFrame
    name: str
    has_fetched: bool

    def __init__(self, name: str) -> None:
        self.current_content = DataFrame()
        self.name = name
        self.has_fetched = False
        pass

    @abc.abstractmethod
    def fetch_current_content(self) -> None:
        pass

    @abc.abstractmethod
    def describe(self) -> str:
        return ""

    def base_string(self) -> str:
        return f"<{self.__class__.__name__}> {self.name}"

    def __str__(self) -> str:
        return f"{self.base_string()} : {self.current_content.__str__()}"

    @abc.abstractmethod
    def persist_changes(
        self,
        row_comparison: DataFrame,
        is_schema_identical: bool,
        new_schema: Schema,
        dry_run: bool,
    ):
        pass
