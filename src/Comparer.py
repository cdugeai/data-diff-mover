from src.input.Input import Input
from src.output.Output import Output

from polars import DataFrame


class Comparer:
    input_: Input
    output_: Output

    def __init__(self, input_: Input, output_: Output) -> None:
        self.input_ = input_
        self.output_ = output_

    def compare(self) -> None:
        if not self.input_.has_load:
            raise RuntimeError("Please, load input data first")
        if not self.output_.has_fetched:
            raise RuntimeError("Please, fetch output data first")

    def __str__(self) -> str:
        return f"Comparer: {self.input_.base_string()} vs {self.output_.base_string()}"

    def helper_compare(self, current_df: DataFrame, new_df: DataFrame) -> None:
        pass
