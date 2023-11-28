import pandas as pd

from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

with fs.transaction as tx:
    tx.create_branch("quickstart", "german-lakes", "main")

    lakes = pd.read_parquet("lakefs://quickstart/main/lakes.parquet")
    german_lakes = lakes.query('Country == "Germany"')
    german_lakes.to_csv("lakefs://quickstart/german-lakes/german_lakes.csv")

    tx.commit("quickstart", "german-lakes", "Add German lakes")
