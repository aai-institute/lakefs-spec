[![](https://img.shields.io/pypi/v/lakefs-spec)](https://pypi.org/project/lakefs-spec) ![GitHub](https://img.shields.io/github/license/aai-institute/lakefs-spec) [![docs](https://img.shields.io/badge/docs-latest-blue)](https://lakefs-spec.org)
 ![GitHub](https://img.shields.io/github/stars/aai-institute/lakefs-spec)

# lakeFS-spec: An fsspec backend for lakeFS

Welcome to lakeFS-spec, a [filesystem-spec](https://github.com/fsspec/filesystem_spec) backend implementation for the [lakeFS](https://lakefs.io/) data lake.
Our primary goal is to streamline versioned data operations in lakeFS, enabling seamless integration with popular data science tools such as Pandas, Polars, and DuckDB directly from Python.

Highlights:

- High-level abstraction over basic lakeFS repository operations
- Seamless integration into the fsspec ecosystem
- Transaction support
- Zero-config option through config autodiscovery
- Automatic up-/download management to avoid unnecessary transfers for unchanged files

> [!NOTE]
> We are seeking early adopters who would like to actively participate in our feedback process and shape the future of the library.
If you are interested in using the library and want to get in touch with us, please reach out via [Github Discussions](https://github.com/aai-institute/lakefs-spec/discussions).

## Installation

lakeFS-spec is published on PyPI, you can simply install it using your favorite package manager:

```shell
$ pip install lakefs-spec
  # or
$ poetry add lakefs-spec
```

## Usage

The following usage examples showcase two major ways of using lakeFS-spec: as a low-level filesystem abstraction, and through third-party (data science) libraries.

For a more thorough overview of the features and use cases for lakeFS-spec, see the [user guide](https://lakefs-spec.org/latest/guides/overview/) and [tutorials](https://lakefs-spec.org/latest/guides/tutorials/) sections in the documentation.

### Low-level: As a fsspec filesystem 

The following example shows how to upload a file, create a commit, and read back the committed data using the bare lakeFS filesystem implementation.
It assumes you have already created a repository named `repo` and have `lakectl` credentials set up on your machine in `~/.lakectl.yaml` (see the [lakeFS quickstart guide](https://docs.lakefs.io/quickstart/) if you are new to lakeFS and need guidance).

```python
from pathlib import Path

from lakefs_spec import LakeFSFileSystem

REPO, BRANCH = "repo", "main"

# Prepare example local data
local_path = Path("demo.txt")
local_path.write_text("Hello, lakeFS!")

# Upload to lakeFS and create a commit
fs = LakeFSFileSystem()  # will auto-discover config from ~/.lakectl.yaml
repo_path = f"{REPO}/{BRANCH}/{local_path.name}"

with fs.transaction as tx:
    fs.put(str(local_path), repo_path)
    tx.commit(REPO, BRANCH, "Add demo data")

# Read back committed file
f = fs.open(repo_path, "rt")
print(f.readline())  # "Hello, lakeFS!"
```

### High-level: Via third-party libraries

A variety of widely-used data science tools are building on fsspec to access remote storage resources and can thus work with lakeFS data lakes directly through lakeFS-spec (see the [fsspec docs](https://filesystem-spec.readthedocs.io/en/latest/#who-uses-fsspec) for details).
The examples assume you have a lakeFS instance with the [`quickstart` repository](https://docs.lakefs.io/quickstart/launch.html) containing sample data available.

```python
# Pandas -- see https://pandas.pydata.org/docs/user_guide/io.html#reading-writing-remote-files
import pandas as pd

data = pd.read_parquet("lakefs://quickstart/main/lakes.parquet")
print(data.head())


# Polars -- see https://pola-rs.github.io/polars/user-guide/io/cloud-storage/
import polars as pl

data = pl.read_parquet("lakefs://quickstart/main/lakes.parquet")
print(data.head())


# DuckDB -- see https://duckdb.org/docs/guides/python/filesystems.html
import duckdb
import fsspec

duckdb.register_filesystem(fsspec.filesystem("lakefs"))
res = duckdb.read_parquet("lakefs://quickstart/main/lakes.parquet")
res.show()
```

## Contributing

We encourage and welcome contributions from the community to enhance the project.
Please check [discussions](https://github.com/aai-institute/lakefs-spec/discussions) or raise an [issue](https://github.com/aai-institute/lakefs-spec/issues) on GitHub for any problems you encounter with the library.

For information on the general development workflow, see the [contribution guide](CONTRIBUTING.md).

## License

The lakeFS-spec library is distributed under the [Apache-2 license](https://github.com/aai-institute/lakefs-spec/blob/main/LICENSE).
