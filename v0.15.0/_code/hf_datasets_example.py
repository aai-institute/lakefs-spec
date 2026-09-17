from datasets import load_dataset

from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

with fs.transaction("quickstart", "main") as tx:
    lakes = load_dataset("parquet", data_files="lakefs://quickstart/main/lakes.parquet")
    irish_lakes = lakes.filter(lambda lake: lake["Country"] == "Ireland")
    irish_lakes.save_to_disk(f"lakefs://quickstart/{tx.branch.id}/irish_lakes")

    tx.commit(message="Add Irish lakes dataset")
