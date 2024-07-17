import polars as pl

from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

with fs.transaction("quickstart", "main") as tx:
    lakes = pl.read_parquet(f"lakefs://quickstart/{tx.branch.id}/lakes.parquet")
    us_lakes = lakes.filter(pl.col("Country") == "United States of America")

    with fs.open(f"lakefs://quickstart/{tx.branch.id}/us_lakes.csv", "wb") as f:
        us_lakes.write_csv(f)  # (1)!

    tx.commit(message="Add US lakes")
