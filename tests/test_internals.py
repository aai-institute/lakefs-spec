import os
from pathlib import Path
from typing import Union

import pytest
from typing_extensions import TypeAlias

from lakefs_spec import LakeFSFileSystem

AnyPath: TypeAlias = Union[str, os.PathLike[str], Path]


@pytest.mark.parametrize(
    "path,expected",
    [
        # base case
        ("lakefs://repo/ref/a.txt", "repo/ref/a.txt"),
        # does not strip trailing slash
        ("lakefs://repo/ref/", "repo/ref/"),
        # can accept Path
        (Path("repo/ref/a.txt"), "repo/ref/a.txt"),
        # Works on lists as well
        (
            ["lakefs://repo/ref/a.txt", "lakefs://repo/ref/b.txt"],
            ["repo/ref/a.txt", "repo/ref/b.txt"],
        ),
    ],
)
def test_strip_protocol(
    fs: LakeFSFileSystem,
    path: Union[AnyPath, list[AnyPath]],
    expected: Union[str, list[str]],
) -> None:
    actual = fs._strip_protocol(path)
    assert actual == expected
