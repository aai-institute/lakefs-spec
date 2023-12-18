import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

with fs.transaction as tx:
    tx.create_branch("quickstart", "partitioned-data", "main")

    lakes_table = pq.read_table("quickstart/main/lakes.parquet", filesystem=fs)

    ds.write_dataset(
        lakes_table,
        "quickstart/partitioned-data/lakes",
        filesystem=fs,
        format="csv",
        partitioning=ds.partitioning(pa.schema([lakes_table.schema.field("Country")])),
    )

    tx.commit("quickstart", "partitioned-data", "Add partitioned lakes data set")
