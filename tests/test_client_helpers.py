import random
import string
import uuid
from typing import Any

import pytest
from lakefs_sdk.exceptions import ApiException, NotFoundException
from lakefs_sdk.models import BranchCreation

import lakefs_spec.client_helpers as client_helpers
from lakefs_spec import LakeFSFileSystem
from tests.util import RandomFileFactory, commit_random_file_on_branch, list_branch_names


def test_create_tag(
    random_file_factory: RandomFileFactory, fs: LakeFSFileSystem, repository: str, temp_branch: str
) -> None:
    commit_random_file_on_branch(
        random_file_factory=random_file_factory,
        fs=fs,
        repository=repository,
        temp_branch=temp_branch,
    )

    tagname = f"Change_{uuid.uuid4()}"
    try:
        tag = client_helpers.create_tag(
            client=fs.client, repository=repository, ref=temp_branch, tag=tagname
        )

        assert tag in client_helpers.list_tags(fs.client, repository)
        existing_tag = client_helpers.create_tag(
            client=fs.client, repository=repository, ref=temp_branch, tag=tagname
        )
        assert tag == existing_tag
    finally:
        fs.client.tags_api.delete_tag(repository=repository, tag=tagname)


def test_cannot_reassign_tag(
    random_file_factory: RandomFileFactory, fs: LakeFSFileSystem, repository: str, temp_branch: str
) -> None:
    commit_random_file_on_branch(
        random_file_factory=random_file_factory,
        fs=fs,
        repository=repository,
        temp_branch=temp_branch,
    )

    tagname = f"Change_{uuid.uuid4()}"
    try:
        client_helpers.create_tag(
            client=fs.client, repository=repository, ref=temp_branch, tag=tagname
        )
        commit_random_file_on_branch(
            random_file_factory=random_file_factory,
            fs=fs,
            repository=repository,
            temp_branch=temp_branch,
        )
        with pytest.raises(ApiException) as e:
            client_helpers.create_tag(
                client=fs.client, repository=repository, ref=temp_branch, tag=tagname
            )
            assert e.status == 409
    finally:
        client_helpers.delete_tag(client=fs.client, repository=repository, tag=tagname)


def test_delete_tag(
    random_file_factory: RandomFileFactory, fs: LakeFSFileSystem, repository: str, temp_branch: str
) -> None:
    commit_random_file_on_branch(
        random_file_factory=random_file_factory,
        fs=fs,
        repository=repository,
        temp_branch=temp_branch,
    )

    tagname = f"Change_{uuid.uuid4()}"
    tag = client_helpers.create_tag(
        client=fs.client, repository=repository, ref=temp_branch, tag=tagname
    )
    assert tag in client_helpers.list_tags(fs.client, repository)
    client_helpers.delete_tag(client=fs.client, repository=repository, tag=tagname)
    assert tag not in client_helpers.list_tags(fs.client, repository)


def test_merge_into_branch(
    random_file_factory: RandomFileFactory,
    fs: LakeFSFileSystem,
    repository: str,
    temp_branch: str,
    temporary_branch_context: Any,
) -> None:
    random_file = random_file_factory.make()

    with temporary_branch_context("merge-into-branch-test") as new_branch:
        source_ref = f"{repository}/{new_branch}/{random_file.name}"
        fs.put(str(random_file), source_ref)
        client_helpers.commit(
            client=fs.client,
            repository=repository,
            branch=new_branch,
            message="Commit new file",
        )

        assert (
            len(
                fs.client.refs_api.diff_refs(
                    repository=repository, left_ref=temp_branch, right_ref=new_branch
                ).results
            )
            > 0
        )
        client_helpers.merge(
            client=fs.client,
            repository=repository,
            source_ref=new_branch,
            target_branch=temp_branch,
        )
        assert (
            len(
                fs.client.refs_api.diff_refs(
                    repository=repository, left_ref=temp_branch, right_ref=new_branch
                ).results
            )
            == 0
        )


def test_merge_into_branch_aborts_on_no_diff(
    caplog: pytest.LogCaptureFixture, fs: LakeFSFileSystem, repository: str, temp_branch: str
) -> None:
    client_helpers.merge(
        client=fs.client, repository=repository, source_ref="main", target_branch=temp_branch
    )
    assert caplog.records[0].message == "No difference between source and target. Aborting merge."


def test_create_repository(fs: LakeFSFileSystem) -> None:
    name = "testrepo" + "".join(random.choices(string.digits, k=8))

    storage_config = fs.client.config_api.get_config().storage_config
    namespace = f"{storage_config.default_namespace_prefix}/{name}"

    repo = None
    try:
        repo = client_helpers.create_repository(fs.client, name=name, storage_namespace=namespace)
        new_repo = client_helpers.create_repository(
            fs.client, name=name, storage_namespace=namespace
        )
        # exist_ok means the same existing repo is returned.
        assert repo == new_repo
    finally:
        if repo is not None:
            fs.client.repositories_api.delete_repository(repo.id)


def test_revert(
    fs: LakeFSFileSystem, random_file_factory: RandomFileFactory, repository: str, temp_branch: str
) -> None:
    random_file = random_file_factory.make()
    source_ref = f"{repository}/{temp_branch}/{random_file.name}"

    fs.put(str(random_file), source_ref)
    client_helpers.commit(
        client=fs.client, repository=repository, branch=temp_branch, message="Commit new file"
    )

    assert (
        len(
            fs.client.refs_api.diff_refs(
                repository=repository, left_ref="main", right_ref=temp_branch
            ).results
        )
        > 0
    )
    client_helpers.revert(client=fs.client, repository=repository, branch=temp_branch)
    assert (
        len(
            fs.client.refs_api.diff_refs(
                repository=repository, left_ref="main", right_ref=temp_branch
            ).results
        )
        == 0
    )


def test_rev_parse(
    fs: LakeFSFileSystem, random_file_factory: RandomFileFactory, repository: str, temp_branch: str
) -> None:
    current_head_commit = fs.client.refs_api.log_commits(
        repository=repository, ref=temp_branch
    ).results[0]
    random_file = random_file_factory.make()
    fs.put(str(random_file), f"{repository}/{temp_branch}/{random_file.name}")
    client_helpers.commit(
        client=fs.client, repository=repository, branch=temp_branch, message="New Commit"
    )

    next_head_commit = fs.client.refs_api.log_commits(
        repository=repository, ref=temp_branch
    ).results[0]

    assert (
        client_helpers.rev_parse(client=fs.client, repository=repository, ref=temp_branch, parent=0)
        == next_head_commit
    )
    assert (
        client_helpers.rev_parse(client=fs.client, repository=repository, ref=temp_branch, parent=1)
        == current_head_commit
    )


def test_rev_parse_error_on_commit_not_found(fs: LakeFSFileSystem, repository: str) -> None:
    non_existent_ref = f"{uuid.uuid4()}"
    with pytest.raises(
        ValueError,
        match=f"{non_existent_ref!r} does not match any revision",
    ):
        client_helpers.rev_parse(
            client=fs.client, repository=repository, ref=non_existent_ref, parent=0
        )


def test_rev_parse_error_on_parent_does_not_exist(
    fs: LakeFSFileSystem, repository: str, temp_branch: str
) -> None:
    n_commits = len(fs.client.refs_api.log_commits(repository=repository, ref=temp_branch).results)
    non_existent_parent = n_commits + 1
    with pytest.raises(
        ValueError,
        match=f"unable to fetch revision {temp_branch}~{non_existent_parent}",
    ):
        client_helpers.rev_parse(
            client=fs.client, repository=repository, ref=temp_branch, parent=non_existent_parent
        )


def test_delete_branch(fs: LakeFSFileSystem, repository: str) -> None:
    temp_branch_name = f"Branch_{uuid.uuid4()}"
    fs.client.branches_api.create_branch(
        repository=repository,
        branch_creation=BranchCreation(name=temp_branch_name, source="main"),
    )
    assert temp_branch_name in list_branch_names(fs, repository)
    client_helpers.delete_branch(fs.client, repository=repository, branch=temp_branch_name)
    assert temp_branch_name not in list_branch_names(fs, repository)


def test_delete_undefined_branch_error(fs: LakeFSFileSystem, repository: str) -> None:
    not_existing_branch_name = f"Branch_{uuid.uuid4()}"
    with pytest.raises(NotFoundException):
        client_helpers.delete_branch(
            fs.client, repository=repository, branch=not_existing_branch_name, missing_ok=False
        )


def test_reset_branch(
    fs: LakeFSFileSystem,
    repository: str,
    random_file_factory: RandomFileFactory,
    temp_branch: str,
) -> None:
    lpath = random_file_factory.make()
    lpath.write_text("Hello")

    rpath = f"lakefs://{repository}/{temp_branch}/{lpath.name}"

    fs.put(str(lpath), rpath)
    client_helpers.reset_branch(fs.client, repository, temp_branch)
    with pytest.raises(FileNotFoundError):
        fs.info(rpath)
