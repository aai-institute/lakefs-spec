from pathlib import Path

import pytest
from lakefs.branch import Branch
from lakefs.repository import Repository

from lakefs_spec import LakeFSFileSystem
from tests.util import RandomFileFactory, put_random_file_on_branch


def test_lakefs_file_open_read(
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
    random_file_factory: RandomFileFactory,
) -> None:
    rpath = put_random_file_on_branch(random_file_factory, fs, repository, temp_branch)
    lpath = str(random_file_factory.path / Path(rpath).name)

    with open(lpath, "rb") as f:
        orig_text = f.read()

    # try opening the remote file
    with fs.open(rpath) as fp:
        text = fp.read()

    assert text == orig_text


def test_lakefs_file_open_write(
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
    random_file_factory: RandomFileFactory,
) -> None:
    rpath = put_random_file_on_branch(random_file_factory, fs, repository, temp_branch)
    lpath = str(random_file_factory.path / Path(rpath).name)

    with open(lpath, "rb") as f:
        orig_text = f.read()

    # try opening the remote file and writing to it
    with fs.open(rpath, "wb") as fp:
        fp.write(orig_text)

    # pulling the written file down again, using ONLY built-in open (!)
    plpath = Path(lpath)
    lpath = plpath.with_name(plpath.name + "_copy")

    blocksize = fs.blocksize
    fs.blocksize = 256
    fs.get(rpath, str(lpath))
    fs.blocksize = blocksize

    with open(lpath, "rb") as f:
        new_text = f.read()

    # round-trip assert.
    assert new_text == orig_text


def test_open_mode_coercion(fs: LakeFSFileSystem, repository: Repository) -> None:
    """Checks that text mode indicators are stripped."""
    with fs.open(f"{repository.id}/main/README.md", "rt") as f:
        assert f.mode == "r"


def test_lakefs_file_unknown_mode(fs: LakeFSFileSystem) -> None:
    """Test that a NotImplementedError is raised on unknown mode encounter."""

    with pytest.raises(NotImplementedError, match="unsupported mode .*"):
        fs.open("hello.py", mode="ab")  # type: ignore
