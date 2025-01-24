from src.input.Input import Input
from polars import read_csv


class CsvInput(Input):
    path: str

    def __init__(self, name: str, path: str) -> None:
        super().__init__(name)
        self.path = path

    def set_path(self, path: str) -> None:
        self.path = path

    def describe(self) -> str:
        return f"{self.base_string()} : filepath -> {self.path}"

    def load(self) -> None:
        self.data = read_csv(self.path)
        self.has_load = True
