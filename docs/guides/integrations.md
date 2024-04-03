# How to use lakeFS-spec with third-party data science libraries

lakeFS-spec is built on top of the fsspec library, which allows third-party libraries to make use of its file system abstraction to offer high-level features.
The [fsspec documentation](https://filesystem-spec.readthedocs.io/en/latest/#who-uses-fsspec){: target="_blank" rel="noopener"} lists examples of its users, mostly data science libraries.

This user guide page adds more detail on how lakeFS-spec can be used with four prominent data science libraries.

!!! tip "Code Examples"
    The code examples assume access to an existing lakeFS server with a `quickstart` repository containing the sample data already set up.

    Please see the [Quickstart guide](../quickstart.md) or [lakeFS quickstart guide](https://docs.lakefs.io/quickstart/launch.html){: target="_blank" rel="noopener"} if you need guidance in getting started.

    The relevant lines for the lakeFS-spec integration in the following code snippets are highlighted.

## Pandas

[Pandas](https://pandas.pydata.org){: target="_blank" rel="noopener"} can read and write data from remote locations, and uses fsspec for all URLs that are not local or HTTP(S).

This means that (almost) all `pd.read_*` and `pd.DataFrame.to_*` operations can benefit from the lakeFS integration offered by our library without any additional configuration.
See the Pandas documentation on [reading/writing remote files](https://pandas.pydata.org/docs/user_guide/io.html#reading-writing-remote-files){: target="_blank" rel="noopener"} for additional details.

The following code snippet illustrates how to read and write Pandas data frames in various formats from/to a lakeFS repository in the context of a [transaction](transactions.md):

```python hl_lines="8 10"
--8<-- "docs/_code/pandas_example.py"
```

## DuckDB

The [DuckDB](https://duckdb.org/){: target="_blank" rel="noopener"} in-memory database management system includes support for fsspec file systems as part of its Python API (see the official documentation on [using fsspec filesystems](https://duckdb.org/docs/guides/python/filesystems.html){: target="_blank" rel="noopener"} for details).
This allows DuckDB to transparently query and store data located in lakeFS repositories through lakeFS-spec.

Similar to the example above, the following code snippet illustrates how to read and write data from/to a lakeFS repository in the context of a [transaction](transactions.md) through the [DuckDB Python API](https://duckdb.org/docs/api/python/overview.html){: target="_blank" rel="noopener"}:

```python hl_lines="6 9 11"
--8<-- "docs/_code/duckdb_example.py"
```

1. Makes the lakeFS-spec file system known to DuckDB (`duckdb.register_filesystem(fsspec.filesystem("lakefs"))` can also be used to avoid the direct import of `LakeFSFileSystem`)

## Hugging Face Datasets

[Hugging Face :hugging_face: Datasets](https://huggingface.co/docs/datasets/index){: target="_blank" rel="noopener"} is a library for easily accessing and sharing datasets for Audio, Computer Vision, and Natural Language Processing (NLP) tasks.
It uses [fsspec internally](https://huggingface.co/docs/datasets/filesystems){: target="_blank" rel="noopener"} to retrieve and store datasets located outside the Hugging Face Hub.

Reading a dataset from a lakeFS repository is as simple as passing the `data_files` argument to the `load_dataset` function with a generic dataset script (e.g., `csv` or `parquet` - see the [docs](https://huggingface.co/docs/datasets/en/loading#local-and-remote-files){: target="_blank" rel="noopener"} for a list of available types).
Datasets can be saved to a lakeFS repository by passing the repository URL to the `save_to_disk()` method of the dataset.

```python hl_lines="8 10"
--8<-- "docs/_code/hf_datasets_example.py"
```

## Polars

!!! warning
    There is an ongoing discussion in the Polars development team whether to remove support for fsspec file systems, with no clear outcome as of the time this page was written.
    Please refer to the discussion on the relevant [GitHub issue](https://github.com/pola-rs/polars/issues/11056){: target="_blank" rel="noopener"} in case you encounter any problems.

The Python API wrapper for the Rust-based [Polars](https://pola-rs.github.io/polars/){: target="_blank" rel="noopener"} DataFrame library can access remote storage through fsspec, similar to Pandas (see the official [documentation on cloud storage](https://pola-rs.github.io/polars/user-guide/io/cloud-storage/){: target="_blank" rel="noopener"}).

Again, the following code example demonstrates how to read a Parquet file and save a modified version back in CSV format to a lakeFS repository from Polars in the context of a  [transaction](transactions.md):

```python hl_lines="8 11-12"
--8<-- "docs/_code/polars_example.py"
```

1. Polars does not support directly writing to remote storage through the `pl.DataFrame.write_*` API (see [docs](https://pola-rs.github.io/polars/user-guide/io/cloud-storage/#writing-to-cloud-storage))

## PyArrow

[Apache Arrow](https://arrow.apache.org/){: target="_blank" rel="noopener"} and its Python API, [PyArrow](https://arrow.apache.org/docs/python/){: target="_blank" rel="noopener"}, can also use fsspec file systems to perform I/O operations on data objects. The documentation has additional details on [using fsspec-compatible file systems with Arrow](https://arrow.apache.org/docs/python/filesystems.html#using-fsspec-compatible-filesystems-with-arrow){: target="_blank" rel="noopener"}.

PyArrow `read_*` and `write_*` functions take an explicit `filesystem` parameter, which accepts any fsspec file system, such as the `LakeFSFileSystem` provided by this library. 

The following example code illustrates the use of lakeFS-spec with PyArrow, reading a Parquet file and writing it back to a lakeFS repository as a partitioned CSV dataset in the context of a [transaction](transactions.md):

```python hl_lines="10 15"
--8<-- "docs/_code/pyarrow_example.py"
```
