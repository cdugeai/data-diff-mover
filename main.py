from src.input.CsvInput import CsvInput

target_file = CsvInput("target_file", "data/target.csv")
print(target_file)
print(target_file.describe())

target_file.load()
print(target_file)

