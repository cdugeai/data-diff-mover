import abc
from polars import DataFrame


class Input:
    data: DataFrame
    name: str
    primary_key: str | None
    use_row_index_as_primary_key: bool

    has_load: bool

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
        self.data = DataFrame()
        self.name = name
        self.primary_key = primary_key
        self.use_row_index_as_primary_key = use_row_index_as_primary_key
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
