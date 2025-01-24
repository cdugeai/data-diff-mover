from src.output.Output import Output
import warnings

from polars import read_csv, DataFrame


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
