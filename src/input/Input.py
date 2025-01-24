import abc
from polars import DataFrame


class Input:
    data: DataFrame
    name: str
    has_load: bool

    def __init__(self, name: str) -> None:  # pragma: no cover
        self.data = DataFrame()
        self.name = name
        self.has_load = False
        pass

    @abc.abstractmethod
    def load(self) -> None:
        pass

    @abc.abstractmethod
    def describe(self) -> str:
        return ""  # pragma: no cover

    def base_string(self) -> str:
        return f"<{self.__class__.__name__}> {self.name}"  # pragma: no cover

    def __str__(self) -> str:
        return f"{self.base_string()} : {self.data.__str__()}"  # pragma: no cover
