import src.from_ as from_
import src.to_ as to_
from dotenv import load_dotenv
import os

load_dotenv()

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


current_content.diff(new_data)

current_content.persist_changes(new_data, dry_run=True)

## Demarches Simplifiées
run_ds_example: bool = True

if run_ds_example:
    # Write all Dossier to csv
    ds = from_.DemarchesSimplifiees(
        "ds",
        demarche_id=int(os.environ.get("DS_DEMARCHE_ID", "")),
        ds_api_token=os.environ.get("DS_API_KEY", ""),
    )
    ds.load()

    ds_out = to_.CsvFile("ds out", "data/output_ds.csv", "dossier_number")
    ds_out.fetch_current_content()
    ds_out.diff(ds, quiet=False)
    ds_out.persist_changes(ds, dry_run=False)
