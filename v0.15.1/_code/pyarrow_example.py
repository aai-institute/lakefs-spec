import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

with fs.transaction("quickstart", "main") as tx:
    lakes_table = pq.read_table(f"quickstart/{tx.branch.id}/lakes.parquet", filesystem=fs)

    ds.write_dataset(
        lakes_table,
        f"quickstart/{tx.branch.id}/lakes",
        filesystem=fs,
        format="csv",
        partitioning=ds.partitioning(pa.schema([lakes_table.schema.field("Country")])),
    )

    tx.commit("Add partitioned lakes data set")
