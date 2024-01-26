import pandas as pd

from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

with fs.transaction("quickstart", "main") as tx:
    lakes = pd.read_parquet(f"lakefs://quickstart/{tx.branch.id}/lakes.parquet")
    german_lakes = lakes.query('Country == "Germany"')
    german_lakes.to_csv(f"lakefs://quickstart/{tx.branch.id}/german_lakes.csv")

    tx.commit(message="Add German lakes")
