# How to use `lakefs-spec` with third-party data science libraries

`lakefs-spec` is built on top of the `fsspec` library, which allows third-party libraries to make use of its file system abstraction to offer high-level features.
The [`fsspec` documentation](https://filesystem-spec.readthedocs.io/en/latest/#who-uses-fsspec){: target="_blank" rel="noopener"} lists examples of its users, mostly data science libraries.

This user guide page adds more detail on how `lakefs-spec` can be used with four prominent data science libraries.

!!! tip "Code Examples"
    The code examples assume access to an existing lakeFS server with a `quickstart` containing the sample data set repository set up.

    Please see the [Quickstart guide](../quickstart.md) if you need guidance in getting started.

## Pandas

[Pandas](https://pandas.pydata.org){: target="_blank" rel="noopener"} can read and write data from remote locations, and uses `fsspec` for all URLs that are not local or HTTP(S).

This means that (almost) all `pd.read_*` and `pd.DataFrame.to_*` operations can benefit from the lakeFS integration offered by our library without any additional configuration.
See the Pandas documentation on [reading/writing remote files](https://pandas.pydata.org/docs/user_guide/io.html#reading-writing-remote-files){: target="_blank" rel="noopener"} for additional details.

The following code snippet illustrates how to read and write Pandas data frames in various formats from/to a lakeFS repository in the context of a [transaction](transactions.md):

```python hl_lines="10 12"
import pandas as pd

from lakefs_spec.transaction import LakeFSFileSystem

fs = LakeFSFileSystem()

with fs.transaction as tx:
    tx.create_branch("quickstart", "german-lakes", "main")

    lakes = pd.read_parquet("lakefs://quickstart/main/lakes.parquet")
    german_lakes = lakes.query('Country == "Germany"')
    german_lakes.to_csv("lakefs://quickstart/german-lakes/german_lakes.csv")

    tx.commit("quickstart", "german-lakes", "Add German lakes")
```

## DuckDB

The [DuckDB](https://duckdb.org/){: target="_blank" rel="noopener"} in-memory database management system includes support for `fsspec` file systems as part of its Python API (see the official documentation on [using fsspec filesystems](https://duckdb.org/docs/guides/python/filesystems.html){: target="_blank" rel="noopener"} for details).
This allows DuckDB to transparently query and store data located in lakeFS repositories through `lakefs-spec`.

Similar to the example above, the following code snippet illustrates how to read and write data from/to a lakeFS repository in the context of a [transaction](transactions.md) through the [DuckDB Python API](https://duckdb.org/docs/api/python/overview.html){: target="_blank" rel="noopener"}:

```python hl_lines="6 11 13"
import duckdb

from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()
duckdb.register_filesystem(fs)  # (1)! 

with fs.transaction as tx:
    tx.create_branch("quickstart", "german-lakes", "main")

    lakes = duckdb.read_parquet("lakefs://quickstart/main/lakes.parquet")
    german_lakes = duckdb.sql("SELECT * FROM lakes where Country='Germany'")
    german_lakes.to_csv("lakefs://quickstart/german-lakes/german_lakes.csv")

    tx.commit("quickstart", "german-lakes", "Add German lakes")
```

1. Makes the `lakefs-spec` file system known to DuckDB (`duckdb.register_filesystem(fsspec.filesystem("lakefs"))` can also be used to avoid the direct import of `LakeFSFileSystem`)

## PyArrow

!!! todo

## Polars

!!! warning
    There is an ongoing discussion in the Polars development team whether to remove support for `fsspec` file systems, with no clear outcome as of the time this page was written.
    Please refer to the discussion on the relevant [GitHub issue](https://github.com/pola-rs/polars/issues/11056){: target="_blank" rel="noopener"} in case you encounter any problems.

The Python API wrapper for the Rust-based [Polars](https://pola-rs.github.io/polars/){: target="_blank" rel="noopener"} DataFrame library can access remote storage through `fsspec`, similar to Pandas (see the official [documentation on cloud storage](https://pola-rs.github.io/polars/user-guide/io/cloud-storage/){: target="_blank" rel="noopener"}).

Again, the following code example demonstrates how to read a Parquet file and save a modified version back in CSV format to a lakeFS repository from Polars in the context of a  [transaction](transactions.md):


```python hl_lines="10 13-14"
import polars as pl

from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

with fs.transaction as tx:
    tx.create_branch("quickstart", "german-lakes", "main")

    lakes = pl.read_parquet("lakefs://quickstart/main/lakes.parquet")
    german_lakes = lakes.filter(pl.col("Country") == "Germany")

    with fs.open("lakefs://quickstart/german-lakes/german_lakes.csv", "wb") as f: # (1)!
        german_lakes.write_csv(f)

    tx.commit("quickstart", "german-lakes", "Add German lakes")
```

1. Polars does not support directly writing to remote storage through the `pl.DataFrame.write_*` API (see [docs](https://pola-rs.github.io/polars/user-guide/io/cloud-storage/#writing-to-cloud-storage))
