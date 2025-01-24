from src.Comparer import Comparer
from src.input.CsvInput import CsvInput
from src.output.CsvOutput import CsvOutput

target_file = CsvInput("target_file", "data/target.csv")
print(target_file)
print(target_file.describe())

target_file.load()
print(target_file)

current_content = CsvOutput("current", "data/current.csv")
print(current_content.describe())

current_content.fetch_current_content()
print(current_content)


c = Comparer(target_file, current_content)
print(c)
c.compare()

target_file.data
