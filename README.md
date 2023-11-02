[![](https://img.shields.io/pypi/v/lakefs-spec)](https://pypi.org/project/lakefs-spec) ![GitHub](https://img.shields.io/github/license/appliedAI-Initiative/lakefs-spec) [![docs](https://img.shields.io/badge/docs-latest-blue)](https://appliedai-initiative.github.io/lakefs-spec/)
 ![GitHub](https://img.shields.io/github/stars/appliedAI-Initiative/lakefs-spec)

# `lakefs-spec`: An `fsspec` backend for lakeFS

The `lakefs-spec` library provides a [filesystem-spec](https://github.com/fsspec/filesystem_spec) backend implementation for the [lakeFS](https://lakefs.io/) data lake.
Its main goal is to facilitate versioned data operations in lakeFS directly from Python code, for example using popular data science tools like `pandas`, `polars`, and `DuckDB`.

## Installation

`lakefs-spec` is published on PyPI, you can simply install it using your favorite package manager:

```shell
$ pip install lakefs-spec
  # or
$ poetry add lakefs-spec
```

If you want `lakefs-spec` to automatically discover and load credentials from an existing `lakectl` credentials file on your machine, additionally install the `PyYAML` library.

## Usage

### As a `fsspec` filesystem 

The following example shows how to upload a file and create a commit using the bare lakeFS filesystem implementation.
It assumes you have already created a repository and have `lakectl` credentials set up on your machine (see the lakeFS quickstart guide for details).

```python
from lakefs_spec import LakeFSFileSystem
from lakefs_spec.client_helpers import commit
from pathlib import Path

REPO, BRANCH = "quickstart", "main"

# Prepare example local data
local_path = Path("demo.txt")
local_path.write_text("Hello, lakeFS!")

# Upload to lakeFS and create a commit
fs = LakeFSFileSystem()  # will auto-discover config from ~/.lakectl.yaml
fs.put(str(local_path), f"{REPO}/{BRANCH}/{local_path.name}")
commit(fs.client, REPO, BRANCH, "Add demo data")
```

### Via third-party libraries

A variety of widely-used data science tools are building on `fsspec` to access remote storage resources and can thus work with lakeFS data lakes directly through `lakefs-spec`:

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

For a more comprehensive overview of third-party tools using `fsspec, see the [TODO: Docs link]().

## Contributing

We welcome contributions to the project!
Please check [discussions](https://github.com/appliedAI-Initiative/lakefs-spec/discussions) or raise an [issue](https://github.com/appliedAI-Initiative/lakefs-spec/issues) on GitHub for any problems you encounter with the library.

For information on the general development workflow, see the [contribution guide](CONTRIBUTING.md).

## License

The `lakefs-spec` project is distributed under the [Apache-2 license](https://github.com/appliedAI-Initiative/lakefs-spec/blob/main/LICENSE).
