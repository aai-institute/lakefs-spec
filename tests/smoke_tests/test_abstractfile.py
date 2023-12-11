from pathlib import Path

import pytest

from lakefs_spec.spec import LakeFSFileSystem
from tests.util import RandomFileFactory


def test_readline(
    fs: LakeFSFileSystem,
    repository: str,
    temp_branch: str,
) -> None:
    """Checks that `fs.open()` behaves like builtin `open` for `readline(s)` APIs."""
    lpath = Path("random_file.txt")
    try:
        lpath.write_text("Hello\nmy name is\nxyz")
        with open(lpath, "rb") as f:
            native_open_line = f.readline()
            f.seek(0)
            native_open_lines = f.readlines()
            f.seek(0)
            firstline = next(f)

        rpath = f"{repository}/{temp_branch}/{Path(lpath).name}"
        fs.put_file(lpath, rpath)

        with fs.open(rpath, "rb") as rf:
            # mode == "rb" means everything is bytes.
            assert rf.readline() == native_open_line
            rf.seek(0)
            assert rf.readlines() == native_open_lines
            rf.seek(0)
            # default char is linebreak (b'\n'),
            # so `readuntil()` emulates the line iterator in its default state.
            assert rf.readuntil() == firstline
    finally:
        lpath.unlink(missing_ok=True)


def test_info(
    random_file_factory: RandomFileFactory, fs: LakeFSFileSystem, repository: str, temp_branch: str
) -> None:
    rnd_file = random_file_factory.make()
    rpath = f"{repository}/{temp_branch}/{rnd_file.name}"
    fs.put(str(rnd_file), rpath)
    details = fs.open(rpath).info()
    expected_keys = ["checksum", "content-type", "mtime", "name", "size", "type"]
    for key in expected_keys:
        assert key in details.keys()

    # opening a nonexistent file in read mode should immediately result in a ``FileNotFoundError``.
    with pytest.raises(FileNotFoundError):
        fs.open(f"{repository}/main/hello.tar.gz", "rb")


def test_readuntil(fs: LakeFSFileSystem, repository: str, temp_branch: str) -> None:
    lpath = Path("tmp_file.txt")

    content_first_part = (
        "This is a test file\nthat contains an occurence until which we want to read:7"
    )
    content_second_part = "\nand some words after it."
    try:
        lpath.write_text(content_first_part + content_second_part)
        rpath = f"{repository}/{temp_branch}/tmp_file.txt"
        fs.put_file(lpath, rpath)
        with fs._open(rpath, "rb") as rf:
            remote_readuntil = rf.readuntil(b"7").decode("utf-8")
            assert content_first_part == remote_readuntil
            assert content_second_part not in remote_readuntil
    finally:
        lpath.unlink(missing_ok=True)
