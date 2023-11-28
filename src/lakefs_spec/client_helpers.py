from __future__ import annotations

import json
import logging

from lakefs_sdk import BranchCreation
from lakefs_sdk.client import LakeFSClient
from lakefs_sdk.exceptions import ApiException, NotFoundException
from lakefs_sdk.models import (
    Commit,
    CommitCreation,
    Ref,
    Repository,
    RepositoryCreation,
    RevertCreation,
    TagCreation,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def commit(
    client: LakeFSClient,
    repository: str,
    branch: str,
    message: str,
    metadata: dict[str, str] | None = None,
) -> Commit:
    diff = client.branches_api.diff_branch(repository=repository, branch=branch)

    if not diff.results:
        logger.warning(f"No changes to commit on branch {branch!r}, aborting commit.")
        return rev_parse(client, repository, branch, parent=0)

    commit_creation = CommitCreation(message=message, metadata=metadata)

    new_commit = client.commits_api.commit(
        repository=repository, branch=branch, commit_creation=commit_creation
    )
    return new_commit


def create_branch(
    client: LakeFSClient, repository: str, name: str, source_branch: str, exist_ok: bool = True
) -> str:
    """
    Create a branch in a lakeFS repository.

    Parameters
    ----------
    client: LakeFSClient
        The lakeFS client configured for (and authenticated with) the target instance.
    repository: str
        Repository name.
    name: str
        Name of the newly created (or existing) branch.
    source_branch: str
        Name of the source branch the new branch is created from.
    exist_ok: bool
        Ignore errors if the branch already exists.

    Returns
    -------
    The requested branch name.
    """

    try:
        new_branch = BranchCreation(name=name, source=source_branch)
        # client.branches_api.create_branch throws ApiException if branch exists
        client.branches_api.create_branch(repository=repository, branch_creation=new_branch)
        logger.debug(f"Created new branch {name!r} from branch {source_branch!r}.")
        return name
    except ApiException as e:
        if e.status == 409 and exist_ok:
            return name
        raise e


def create_repository(
    client: LakeFSClient, name: str, storage_namespace: str, exist_ok: bool = True
) -> Repository:
    """
    Create a repository with the given name and storage namespace.

    Important: Due to cleanup issues in the lakeFS backend, creating a repository again after prior
    deletion under the same name and storage namespace will almost certainly not work.

    The `exist_ok` flag only asserts idempotence in case the repository is not deleted in between
    `create_repository` calls.
    """
    try:
        repository_creation = RepositoryCreation(name=name, storage_namespace=storage_namespace)
        return client.repositories_api.create_repository(repository_creation=repository_creation)
    except ApiException as e:
        if exist_ok:
            msg: str = json.loads(e.body)["message"]
            if e.status == 400 and msg.endswith("namespace already in use") or e.status == 409:
                return client.repositories_api.get_repository(name)
        raise e


def create_tag(
    client: LakeFSClient, repository: str, ref: str | Commit, tag: str, exist_ok: bool = True
) -> Ref:
    if isinstance(ref, Commit):
        ref = ref.id
    tag_creation = TagCreation(id=tag, ref=ref)

    try:
        return client.tags_api.create_tag(repository=repository, tag_creation=tag_creation)
    except ApiException as e:
        if e.status == 409 and exist_ok:
            return client.tags_api.get_tag(repository=repository, tag=tag)
        raise e


def list_tags(client: LakeFSClient, repository: str) -> list[Ref]:
    return client.tags_api.list_tags(repository=repository).results


def merge(client: LakeFSClient, repository: str, source_ref: str, target_branch: str) -> None:
    diff = client.refs_api.diff_refs(
        repository=repository, left_ref=target_branch, right_ref=source_ref
    )
    if not diff.results:
        logger.warning("No difference between source and target. Aborting merge.")
        return
    client.refs_api.merge_into_branch(
        repository=repository, source_ref=source_ref, destination_branch=target_branch
    )


def revert(client: LakeFSClient, repository: str, branch: str, parent_number: int = 1) -> None:
    """Reverts the commit on the specified branch to the parent specified by parent_number.

    Parameters
    ----------
    client: LakeFSClient
        The client to interact with.
    repository: str
        Repository in which the specified branch is located.
    branch: str
        Branch on which the commit should be reverted.
    parent_number: int, optional
        If there are multiple parents to a commit, specify to which parent the commit should be reverted. Index starts at 1. Defaults to 1.
    """
    revert_creation = RevertCreation(ref=branch, parent_number=parent_number)
    client.branches_api.revert_branch(
        repository=repository, branch=branch, revert_creation=revert_creation
    )


def rev_parse(
    client: LakeFSClient,
    repository: str,
    ref: str | Commit,
    parent: int = 0,
) -> Commit:
    if parent < 0:
        raise ValueError(f"Parent cannot be negative, got {parent}")
    try:
        if isinstance(ref, Commit):
            ref = ref.id
        revisions = client.refs_api.log_commits(
            repository=repository, ref=ref, limit=True, amount=2 * (parent + 1)
        ).results
        if len(revisions) <= parent:
            raise ValueError(
                f"cannot fetch revision {ref}~{parent}: "
                f"ref {ref!r} only has {len(revisions)} parents"
            )
        return revisions[parent]
    except NotFoundException:
        raise ValueError(f"{ref!r} does not match any revision in lakeFS repository {repository!r}")
