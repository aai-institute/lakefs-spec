import filecmp
import tempfile

import pytest

from lakefs_spec.spec import LakeFSFileSystem
from tests.util import RandomFileFactory


def test_walk_single_dir(fs: LakeFSFileSystem, repository: str) -> None:
    """`walk` in a single directory should find all files contained therein"""
    branch = "main"
    resource = "images"
    path = f"{repository}/{branch}/{resource}/"

    dirname, dirs, files = next(fs.walk(path))
    assert dirname == path
    assert dirs == []
    assert len(files) == 37  # NOTE: hardcoded for quickstart repo


def test_walk_repo_root(fs: LakeFSFileSystem, repository: str) -> None:
    """`walk` should be able to be called on the root directory of a repository"""
    branch = "main"
    path = f"{repository}/{branch}/"

    dirname, dirs, files = next(fs.walk(path))
    assert dirname == path
    assert len(dirs) == 2
    assert len(files) == 2


def test_find_in_folder(fs: LakeFSFileSystem, repository: str) -> None:
    path = f"{repository}/main/"
    # Find the 37 elements in images directory in test repo
    assert len(fs.find(path + "images")) == 37


def test_touch(
    fs: LakeFSFileSystem,
    repository: str,
    temp_branch: str,
) -> None:
    rpath = f"{repository}/{temp_branch}/hello.txt"
    fs.touch(rpath)
    assert fs.exists(rpath)

    # mock the server config to an older version.
    fs._lakefs_server_version = (1, 0, 0)

    with pytest.raises(NotImplementedError, match=r"\.touch\(\) is not supported"):
        fs.touch(rpath)


def test_glob(
    fs: LakeFSFileSystem,
    repository: str,
) -> None:
    branch = "main"
    files = fs.glob(f"lakefs://{repository}/{branch}/**/*.png")
    assert len(files) > 0


def test_du(
    fs: LakeFSFileSystem,
    repository: str,
) -> None:
    branch = "main"
    size = fs.du(f"lakefs://{repository}/{branch}/", withdirs=True)
    assert size > 2**20  # at least 1 MiB in the quickstart repo


def test_size(
    fs: LakeFSFileSystem,
    repository: str,
) -> None:
    branch = "main"
    size = fs.size(f"lakefs://{repository}/{branch}/lakes.parquet")
    assert size >= 2**19  # lakes.parquet is larger than 500 KiB


def test_isfile_isdir(
    fs: LakeFSFileSystem,
    repository: str,
) -> None:
    branch = "main"
    assert fs.isfile(f"lakefs://{repository}/{branch}/lakes.parquet")
    assert not fs.isdir(f"lakefs://{repository}/{branch}/lakes.parquet")

    assert not fs.isfile(f"lakefs://{repository}/{branch}/data")
    assert fs.isdir(f"lakefs://{repository}/{branch}/data")


def test_write_text_read_text(
    fs: LakeFSFileSystem,
    repository: str,
    temp_branch: str,
) -> None:
    rpath = f"lakefs://{repository}/{temp_branch}/new-file.txt"
    content = "Hello, World!"
    encoding = "utf-32-le"  # use a non-standard encoding

    fs.write_text(rpath, content, encoding=encoding)
    actual = fs.read_text(rpath, encoding=encoding)
    assert actual == content


def test_cat_pipe(
    fs: LakeFSFileSystem,
    repository: str,
    temp_branch: str,
) -> None:
    rpath = f"lakefs://{repository}/{temp_branch}/new-file.txt"
    content = "Hello, World!"
    encoding = "utf-32-le"  # use a non-standard encoding
    fs.pipe(rpath, content.encode(encoding))

    actual = fs.cat(rpath)
    assert str(actual, encoding=encoding) == content

    actual = fs.cat_file(rpath, end=4)  # Only fetch first UTF-32 glyph
    assert str(actual, encoding=encoding) == content[0]


def test_cat_pipe_file(
    fs: LakeFSFileSystem,
    repository: str,
    temp_branch: str,
) -> None:
    rpath = f"lakefs://{repository}/{temp_branch}/new-file.txt"
    content = "Hello, World!"
    encoding = "utf-32-le"  # use a non-standard encoding
    fs.pipe_file(rpath, content.encode(encoding))

    actual = fs.cat(rpath)
    assert str(actual, encoding=encoding) == content


def test_cat_ranges(
    fs: LakeFSFileSystem,
    repository: str,
    temp_branch: str,
) -> None:
    rpaths = [f"lakefs://{repository}/{temp_branch}/file-{idx}.txt" for idx in range(2)]
    content = "Hello, World!"
    encoding = "utf8"

    fs.write_text(rpaths[0], content, encoding=encoding)
    fs.write_text(rpaths[1], content, encoding=encoding)

    # fetch first byte of each file
    ranges = fs.cat_ranges(rpaths, starts=0, ends=1)
    for idx in range(2):
        assert str(ranges[idx], encoding=encoding) == content[0]


def test_head_tail(
    fs: LakeFSFileSystem,
    repository: str,
    temp_branch: str,
) -> None:
    rpath = f"lakefs://{repository}/{temp_branch}/new-file.txt"
    content = "Hello, World!"
    encoding = "utf-8"

    fs.write_text(rpath, content, encoding=encoding)

    size = 2
    head = fs.head(rpath, size=size)
    assert str(head, encoding=encoding) == content[:size]

    tail = fs.tail(rpath, size=size)
    assert str(tail, encoding=encoding) == content[len(content) - size :]


def test_mv(
    random_file_factory: RandomFileFactory,
    fs: LakeFSFileSystem,
    repository: str,
    temp_branch: str,
) -> None:
    random_file = random_file_factory.make()
    lpath = str(random_file)
    rpath1 = f"{repository}/{temp_branch}/new_dir/{random_file.name}"
    rpath2 = f"{repository}/{temp_branch}/{random_file.name}"

    fs.put_file(lpath=lpath, rpath=rpath1)
    assert fs.exists(rpath1)
    assert not fs.exists(rpath2)

    fs.mv(rpath1, rpath2)
    assert not fs.exists(rpath1)
    assert fs.exists(rpath2)


def test_copy(
    random_file_factory: RandomFileFactory,
    fs: LakeFSFileSystem,
    repository: str,
    temp_branch: str,
) -> None:
    random_file = random_file_factory.make()
    lpath = str(random_file)
    rpath1 = f"{repository}/{temp_branch}/new_dir/{random_file.name}"
    rpath2 = f"{repository}/{temp_branch}/{random_file.name}"

    fs.put_file(lpath=lpath, rpath=rpath1)
    assert fs.exists(rpath1)
    assert not fs.exists(rpath2)

    fs.cp(rpath1, rpath2)
    assert fs.exists(rpath1)
    assert fs.exists(rpath2)

    assert fs.info(rpath1)["checksum"] == fs.info(rpath2)["checksum"]


def test_get_file(
    random_file_factory: RandomFileFactory, fs: LakeFSFileSystem, repository: str, temp_branch: str
) -> None:
    try:
        random_file = random_file_factory.make()
        lpath1 = str(random_file)
        rpath = f"{repository}/{temp_branch}/{random_file.name}"
        fs.put(lpath=lpath1, rpath=rpath)

        tmp_dir = tempfile.TemporaryDirectory()
        lpath2 = f"{tmp_dir.name}/{random_file.name}"
        fs.get(rpath=rpath, lpath=lpath2)
        assert filecmp.cmp(lpath1, lpath2)
    finally:
        tmp_dir.cleanup()
