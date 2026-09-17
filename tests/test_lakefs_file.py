import random
import string
from pathlib import Path

import lakefs
import lakefs_sdk
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


def test_lakefs_file_open_pre_sign_none_uses_storage_config(
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
    random_file_factory: RandomFileFactory,
) -> None:
    """Test that pre_sign=None uses the storage configuration's pre_sign_support value."""
    rpath = put_random_file_on_branch(random_file_factory, fs, repository, temp_branch)

    with fs.open(rpath, mode="rb", pre_sign=None) as fp:
        client = fp._client
        if hasattr(client, "storage_config_by_id"):
            expected_pre_sign = client.storage_config_by_id(fp._obj.storage_id()).pre_sign_support
        else:
            expected_pre_sign = client.storage_config.pre_sign_support

        # Verify that the file object's pre_sign property matches the storage config
        assert fp.pre_sign == expected_pre_sign


def test_write_to_protected_branch_raises_permission_error(fs: LakeFSFileSystem) -> None:
    """Regression test for GH-296: lakeFS API errors on close() must be translated."""
    # NB: Use a fresh repository, since lakeFS caches branch protection rules per repository
    # for a few seconds, so that a rule set on the shared test repository is not enforced immediately.
    client = fs.client
    name = "protected-" + "".join(random.choices(string.digits, k=8))
    storage_namespace = f"{client.storage_config.default_namespace_prefix}/{name}"
    repo = lakefs.Repository(name, client=client).create(storage_namespace=storage_namespace)
    try:
        client.sdk_client.repositories_api.set_branch_protection_rules(
            repo.id, [lakefs_sdk.BranchProtectionRule(pattern="main")]
        )
        rpath = f"{repo.id}/main/protected.txt"
        with (
            pytest.raises(
                PermissionError, match=f"^403 cannot write to protected branch: {rpath!r}$"
            ) as excinfo,
            fs.open(rpath, "wb") as f,
        ):
            f.write(b"data")
        # the original lakeFS error remains available as the cause
        assert excinfo.value.__cause__ is not None
        assert getattr(excinfo.value.__cause__, "status_code", None) == 403
    finally:
        repo.delete()


def test_read_of_deleted_object_raises_file_not_found(
    random_file_factory: RandomFileFactory,
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    """lakeFS API errors on read() must be translated, since reads are lazy."""
    rpath = put_random_file_on_branch(random_file_factory, fs, repository, temp_branch)

    f = fs.open(rpath, "rb")
    fs.rm(rpath)
    with pytest.raises(FileNotFoundError, match=repr(rpath)):
        f.read()


def test_raw_sdk_exceptions_are_translated(
    random_file_factory: RandomFileFactory,
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    """
    Some lakeFS SDK code paths (e.g. presigned uploads) raise raw
    ``lakefs_sdk.ApiException``s instead of ``lakefs.exceptions.ServerException``s.
    These must be translated as well.
    """
    rpath = f"{repository.id}/{temp_branch.id}/raw.txt"
    with fs.open(rpath, "wb", pre_sign=False) as f:
        with pytest.raises(PermissionError, match=rf"^403\b.*{rpath!r}$"), f._translate_errors():
            raise lakefs_sdk.ApiException(status=403, reason="Forbidden")
        f.discard()
