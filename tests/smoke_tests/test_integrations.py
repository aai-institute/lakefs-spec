import duckdb
import numpy as np
import pandas as pd
import polars as pl
from lakefs.branch import Branch
from lakefs.repository import Repository

from lakefs_spec.spec import LakeFSFileSystem

storage_options = dict(
    host="localhost:8000",
    username="AKIAIOSFOLQUICKSTART",
    password="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
)


def test_pandas_integration(
    fs: LakeFSFileSystem, repository: Repository, temp_branch: Branch
) -> None:
    """Assure the correctness of pandas DataFrame reads and writes, which use `fs.open()`."""
    df = pd.read_parquet(
        f"lakefs://{repository.id}/{temp_branch.id}/lakes.parquet", storage_options=storage_options
    )
    df["randomcol"] = np.random.randn(len(df.index))
    df.to_parquet(
        f"lakefs://{repository.id}/{temp_branch.id}/lakes_new.parquet",
        storage_options=storage_options,
    )
    assert fs.exists(f"lakefs://{repository.id}/{temp_branch.id}/lakes_new.parquet")


def test_polars_integration(repository: Repository) -> None:
    """Test the download and instantiation of polars DataFrames via `fs.open()`."""
    pl.read_parquet(
        f"lakefs://{repository.id}/main/lakes.parquet",
        use_pyarrow=True,
        storage_options=storage_options,
    )


def test_duckdb_integration(fs: LakeFSFileSystem, repository: Repository) -> None:
    """Test the correct registration of the lakeFS file system in duckDB."""
    # see https://duckdb.org/docs/guides/python/filesystems.html
    duckdb.register_filesystem(fs)
    duckdb.read_parquet(f"lakefs://{repository.id}/main/lakes.parquet")
