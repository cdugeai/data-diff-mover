from src.Input import Input
from polars import read_csv


class CsvFile(Input):
    path: str

    def __init__(
        self,
        name: str,
        path: str,
        primary_key: str | None = None,
        use_row_index_as_primary_key: bool = False,
    ) -> None:
        super().__init__(
            name=name,
            primary_key=primary_key,
            use_row_index_as_primary_key=use_row_index_as_primary_key,
        )
        self.path = path

    def describe(self) -> str:  # pragma: no cover
        return f"{self.base_string()} : filepath -> {self.path}"

    def load(self) -> None:
        self.data = read_csv(self.path)
        self.has_load = True
