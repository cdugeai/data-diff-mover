from src.input.Input import Input
from src.output.Output import Output

from polars import DataFrame, Schema


class Comparer:
    input_: Input
    output_: Output

    is_identical_schema: bool

    def __init__(self, input_: Input, output_: Output) -> None:
        self.input_ = input_
        self.output_ = output_

    def compare(self) -> None:
        if not self.input_.has_load:
            raise RuntimeError("Please, load input data first")
        if not self.output_.has_fetched:
            raise RuntimeError("Please, fetch output data first")

        self.is_identical_schema = self.compare_schema()

    def __str__(self) -> str:
        return f"Comparer: {self.input_.base_string()} vs {self.output_.base_string()}"

    def compare_schema(self) -> bool:
        schema_current: Schema = self.output_.current_content.collect_schema()
        schema_new: Schema = self.input_.data.collect_schema()

        schema_are_identical: bool = schema_current.__eq__(schema_new)
        if schema_are_identical:
            print("Les schemas sont identiques")
        else:
            print("Les schemas sont différents")
            print("old schema", schema_current)
            print("new schema", schema_new)

        return schema_are_identical
