import abc
from polars import DataFrame


class Input:
    data: DataFrame
    name: str
    has_load: bool

    def __init__(self, name: str) -> None:
        self.data = DataFrame()
        self.name = name
        self.has_load = False
        pass

    @abc.abstractmethod
    def load(self) -> None:
        pass

    @abc.abstractmethod
    def describe(self) -> str:
        return ""

    def base_string(self) -> str:
        return f"<{self.__class__.__name__}> {self.name}"

    def __str__(self) -> str:
        return f"{self.base_string()} : {self.data.__str__()}"
