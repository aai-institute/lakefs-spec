"""
Static typing checks for the public API. These functions are never executed;
they are verified by the type checker (ty) as part of the pre-commit hooks.
"""

from typing import assert_type

from lakefs_spec import LakeFSFileSystem
from lakefs_spec.file import LakeFSObjectReader, LakeFSObjectWriter


def _check_open_return_types(fs: LakeFSFileSystem, path: str) -> None:
    assert_type(fs.open(path), LakeFSObjectReader[bytes])
    assert_type(fs.open(path, "rb"), LakeFSObjectReader[bytes])
    assert_type(fs.open(path, "r"), LakeFSObjectReader[str])
    assert_type(fs.open(path, "rt"), LakeFSObjectReader[str])
    assert_type(fs.open(path, "wb"), LakeFSObjectWriter[bytes])
    assert_type(fs.open(path, "xb"), LakeFSObjectWriter[bytes])
    assert_type(fs.open(path, "w"), LakeFSObjectWriter[str])
    assert_type(fs.open(path, "xt"), LakeFSObjectWriter[str])


def _check_io_types(fs: LakeFSFileSystem, path: str) -> None:
    with fs.open(path) as fb:
        assert_type(fb, LakeFSObjectReader[bytes])
        assert_type(fb.read(), bytes)
        assert_type(fb.readline(), bytes)
        for line in fb:
            assert_type(line, bytes)

    with fs.open(path, "r") as ft:
        assert_type(ft.read(), str)
        assert_type(ft.readline(), str)

    with fs.open(path, "wb") as wb:
        wb.write(b"bytes")
        wb.write("str")  # ty: ignore[invalid-argument-type]

    with fs.open(path, "w") as wt:
        wt.write("str")
        wt.write(b"bytes")  # ty: ignore[invalid-argument-type]
