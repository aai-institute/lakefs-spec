import polars as pl

from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

with fs.transaction as tx:
    tx.create_branch("quickstart", "us-lakes", "main")

    lakes = pl.read_parquet("lakefs://quickstart/main/lakes.parquet")
    us_lakes = lakes.filter(pl.col("Country") == "United States of America")

    with fs.open("lakefs://quickstart/us-lakes/us_lakes.csv", "wb") as f:
        us_lakes.write_csv(f)

    tx.commit("quickstart", "us-lakes", "Add US lakes")
