import src.from_ as from_
import src.to_ as to_
from polars import read_csv, DataFrame
from polars.testing import assert_frame_equal

from pathlib import Path

IN_CONTENT: str = """a,b
1,2
3,4
"""

OUT_CONTENT: str = """a,b
1,2
"""


def test_persist_csv_to_csv() -> None:
    IN_FILE = "/tmp/in.csv"
    OUT_FILE = "/tmp/out.csv"

    # Fixture: write in files
    Path(IN_FILE).write_text(IN_CONTENT, encoding="utf-8")
    Path(OUT_FILE).write_text(OUT_CONTENT, encoding="utf-8")

    infile: from_.CsvFile = from_.CsvFile(
        "test input file",
        IN_FILE,
        primary_key="a",
    )
    infile.load()
    outfile: to_.CsvFile = to_.CsvFile(
        "test output file",
        OUT_FILE,
        primary_key="a",
    )

    outfile.persist_changes(infile, dry_run=False)

    input_df: DataFrame = read_csv(IN_FILE)
    persisted_df: DataFrame = read_csv(OUT_FILE)

    # Fixture: delete files
    Path(IN_FILE).unlink()
    Path(OUT_FILE).unlink()

    assert_frame_equal(input_df, persisted_df)
