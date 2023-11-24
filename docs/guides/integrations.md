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

```python
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

Similar to the example above, the following code snippet illustrates how to read and write data from/to a lakeFS repository in the context of a [transaction](transactions.md) through the DuckDB Python API:

```python
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

!!! todo
