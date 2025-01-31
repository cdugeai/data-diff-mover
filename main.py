from src.Comparer import Comparer
import src.from_ as from_
import src.to_ as to_


new_data = from_.CsvFile(
    "new_data", "data/new_data.csv", use_row_index_as_primary_key=True
)
print(new_data)
print(new_data.describe())

new_data.load()
print(new_data)

current_content = to_.CsvFile(
    "current", "data/current.csv", use_row_index_as_primary_key=True
)
print(current_content.describe())

current_content.fetch_current_content()
print(current_content)


c = Comparer(new_data, current_content)
print(c)
c.compare()

c.persist_compare(dry_run=True)
