import duckdb

from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()
duckdb.register_filesystem(fs)

with fs.transaction("quickstart", "main") as tx:
    lakes = duckdb.read_parquet("lakefs://quickstart/main/lakes.parquet")
    italian_lakes = duckdb.sql("SELECT * FROM lakes where Country='Italy'")
    italian_lakes.to_csv(f"lakefs://quickstart/{tx.branch.id}/italian_lakes.csv")

    tx.commit(message="Add Italian lakes")
