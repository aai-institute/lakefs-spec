import uuid
from typing import Any

import pytest

import lakefs_spec.client_helpers as client_helpers
from lakefs_spec import LakeFSFileSystem
from tests.util import RandomFileFactory


def test_create_tag(
    random_file_factory: RandomFileFactory, fs: LakeFSFileSystem, repository: str, temp_branch: str
) -> None:
    random_file = random_file_factory.make()
    lpath = str(random_file)
    rpath = f"{repository}/{temp_branch}/{random_file.name}"
    fs.put(lpath, rpath, precheck=False)
    client_helpers.commit(
        client=fs.client, repository=repository, branch=temp_branch, message="Commit File Factory"
    )

    tag = f"Change_{uuid.uuid4()}"
    try:
        client_helpers.create_tag(client=fs.client, repository=repository, ref=temp_branch, tag=tag)

        assert any(commit.id == tag for commit in fs.client.tags_api.list_tags(repository).results)
    finally:
        fs.client.tags_api.delete_tag(repository=repository, tag=tag)


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
        with fs.scope(create_branch_ok=True):
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


def test_revert(
    fs: LakeFSFileSystem, random_file_factory: RandomFileFactory, repository: str, temp_branch: str
) -> None:
    random_file = random_file_factory.make()
    source_ref = f"{repository}/{temp_branch}/{random_file.name}"
    with fs.scope(create_branch_ok=True):
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
    current_head_commit = (
        fs.client.refs_api.log_commits(repository=repository, ref=temp_branch).results[0].id
    )
    random_file = random_file_factory.make()
    fs.put(str(random_file), f"{repository}/{temp_branch}/{random_file.name}")
    client_helpers.commit(
        client=fs.client, repository=repository, branch=temp_branch, message="New Commit"
    )

    next_head_commit = (
        fs.client.refs_api.log_commits(repository=repository, ref=temp_branch).results[0].id
    )

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
        RuntimeError,
        match=f"{non_existent_ref!r} does not match any revision in lakeFS repository {repository!r}",
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
        match=f"cannot fetch revision {temp_branch}~{non_existent_parent}: {temp_branch} only has {n_commits} parents",
    ):
        client_helpers.rev_parse(
            client=fs.client, repository=repository, ref=temp_branch, parent=non_existent_parent
        )
