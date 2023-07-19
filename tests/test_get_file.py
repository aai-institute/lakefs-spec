from pathlib import Path

import pytest

from lakefs_spec.client import LakeFSClient
from lakefs_spec.spec import LakeFSFileSystem


def test_get_nonexistent_file(lakefs_client: LakeFSClient, repository: str) -> None:
    """
    Regression test against error on file closing in fs.get_file() after a
     lakeFS API exception.
    """
    fs = LakeFSFileSystem(client=lakefs_client, repository=repository)

    with pytest.raises(FileNotFoundError):
        fs.get_file("hello-i-no-exist1234.txt", "out.txt", ref="main")

    Path("out.txt").unlink(missing_ok=True)
