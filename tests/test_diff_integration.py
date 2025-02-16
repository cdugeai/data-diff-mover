import src.from_ as from_
import src.to_ as to_
from pytest import raises

from src.exceptions import NoLoadedData
from pathlib import Path
from polars.testing import assert_frame_equal


def test_diff_no_data_input() -> None:
    with raises(
        NoLoadedData, match="Please, load data first in dataset: test input file"
    ):
        infile: from_.CsvFile = from_.CsvFile(
            "test input file",
            "/tmp/in.csv",
            primary_key=None,
            use_row_index_as_primary_key=True,
        )
        outfile: to_.CsvFile = to_.CsvFile(
            "test output file",
            "/tmp/out.csv",
            primary_key=None,
            use_row_index_as_primary_key=True,
        )

        outfile.persist_changes(infile, dry_run=True)


IN_CONTENT: str = """a,b
1,2
3,45
"""


def test_diff_no_data_output() -> None:
    IN_FILE = "/tmp/in.csv"
    OUT_FILE = "/tmp/out.csv"

    Path(IN_FILE).unlink(missing_ok=True)
    Path(IN_FILE).write_text(IN_CONTENT, encoding="utf-8")
    infile: from_.CsvFile = from_.CsvFile(
        "test input file",
        IN_FILE,
        primary_key=None,
        use_row_index_as_primary_key=True,
    )
    infile.load()
    Path(OUT_FILE).unlink(missing_ok=True)
    outfile: to_.CsvFile = to_.CsvFile(
        "test output file",
        OUT_FILE,
        primary_key=None,
        use_row_index_as_primary_key=True,
    )
    outfile.persist_changes(infile, dry_run=False)

    outfile.fetch_current_content()
    assert_frame_equal(infile.data, outfile.current_content)

    Path(IN_FILE).unlink()
    Path(OUT_FILE).unlink()
