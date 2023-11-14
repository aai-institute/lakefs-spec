import pytest

import lakefs_spec.spec
from lakefs_spec import LakeFSFileSystem
from tests.util import RandomFileFactory


def test_lakefs_file_open_read(
    fs: LakeFSFileSystem,
    repository: str,
    temp_branch: str,
    random_file_factory: RandomFileFactory,
) -> None:
    random_file = random_file_factory.make()
    with open(random_file, "rb") as f:
        orig_text = f.read()

    lpath = str(random_file)
    rpath = f"{repository}/{temp_branch}/{random_file.name}"
    fs.put_file(lpath, rpath)

    # try opening the remote file
    with fs.open(rpath) as fp:
        text = fp.read()

    assert text == orig_text


def test_lakefs_file_open_write(
    fs: LakeFSFileSystem,
    repository: str,
    temp_branch: str,
    random_file_factory: RandomFileFactory,
) -> None:
    random_file = random_file_factory.make()
    with open(random_file, "rb") as f:
        orig_text = f.read()

    rpath = f"{repository}/{temp_branch}/{random_file.name}"

    lakefs_spec.spec._warn_on_fileupload = True

    with pytest.warns(
        UserWarning,
        match=r"Calling `LakeFSFileSystem\.open\(\)` in write mode results in unbuffered file uploads.*",
    ):
        # try opening the remote file and writing to it
        with fs.open(rpath, "wb") as fp:
            fp.write(orig_text)

    # pulling the written file down again, using ONLY built-in open (!)
    lpath = random_file.with_name(random_file.name + "_copy")

    blocksize = fs.blocksize
    fs.blocksize = 256
    fs.get(rpath, str(lpath))
    fs.blocksize = blocksize

    with open(lpath, "rb") as f:
        new_text = f.read()

    # round-trip assert.
    assert new_text == orig_text


def test_lakefs_file_unknown_mode(fs: LakeFSFileSystem) -> None:
    """Test that a NotImplementedError is raised on unknown mode encounter."""

    with pytest.raises(NotImplementedError, match="unsupported mode .*"):
        fs.open("hello.py", mode="ab")
