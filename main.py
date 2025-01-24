from src.Comparer import Comparer
from src.input.CsvInput import CsvInput
from src.output.CsvOutput import CsvOutput

new_data = CsvInput("new_data", "data/new_data.csv")
print(new_data)
print(new_data.describe())

new_data.load()
print(new_data)

current_content = CsvOutput("current", "data/current.csv")
print(current_content.describe())

current_content.fetch_current_content()
print(current_content)


c = Comparer(new_data, current_content)
print(c)
c.compare()

c.persist_compare(dry_run=True)
