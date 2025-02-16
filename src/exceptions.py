class ColumnNameIsNotPK(Exception):
    """Raised when a given column name is not actually a primary key when looking at the data"""

    def __init__(self, column_name: str) -> None:
        super().__init__("Following column is not a primary key: " + column_name)


class NoLoadedData(Exception):
    """Raised when try to operate Input or Output without loading data"""

    def __init__(self, dataset_name: str) -> None:
        super().__init__("Please, load data first in dataset: " + dataset_name)
