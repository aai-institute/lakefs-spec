import duckdb

from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()
duckdb.register_filesystem(fs)

with fs.transaction as tx:
    tx.create_branch("quickstart", "italian-lakes", "main")

    lakes = duckdb.read_parquet("lakefs://quickstart/main/lakes.parquet")
    italian_lakes = duckdb.sql("SELECT * FROM lakes where Country='Italy'")
    italian_lakes.to_csv("lakefs://quickstart/italian-lakes/italian_lakes.csv")

    tx.commit("quickstart", "italian-lakes", "Add Italian lakes")
