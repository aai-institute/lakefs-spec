"""
This module provides a collection of functions to interact with a lakeFS server using the lakeFS SDK.
It includes functionalities to manage branches, repositories, commits, tags, and merge operations in
a lakeFS instance.

Note: Users of this module should have a configured and authenticated LakeFSClient instance, which is
required input to all functions.
"""

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
    ResetCreation,
    RevertCreation,
    TagCreation,
)

from lakefs_spec.util import depaginate

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def commit(
    client: LakeFSClient,
    repository: str,
    branch: str,
    message: str,
    metadata: dict[str, str] | None = None,
) -> Commit:
    """
    Create a new commit of all uncommitted changes on the branch in the lakeFS file storage.

    Parameters
    ----------
    client: LakeFSClient
        lakeFS client object.
    repository: str
        Repository name.
    branch: str
        Branch name.
    message: str
        Commit message.
    metadata: dict[str, str] | None
        Additional metadata for the commit.

    Returns
    -------
    Commit
        The created commit object of the lakeFS server.
    """
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
    Create a branch ``name`` in a lakeFS repository.

    Parameters
    ----------
    client: LakeFSClient
        lakeFS client object.
    repository: str
        Repository name.
    name: str
        Name of the branch to be created.
    source_branch: str
        Name of the source branch the new branch is created from.
    exist_ok: bool
        Ignore creation errors if the branch already exists.

    Returns
    -------
    str
        Name of newly created or existing branch.

    Raises
    ------
    ApiException
        If a branch with the same name already exists and ``exist_ok=False``.
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


def delete_branch(
    client: LakeFSClient, repository: str, branch: str, missing_ok: bool = False
) -> None:
    """
    Delete the specified branch from a repository.

    Parameters
    ----------
    client: LakeFSClient
        lakeFS client object.
    repository: str
        Name of the repository from which the branch will be deleted.
    branch: str
        Name of the branch to be deleted.
    missing_ok: bool
        Ignore errors if the requested branch does not exist in the repository.

    Raises
    ------
    NotFoundException
        If the branch does not exist in the repository and ``missing_ok=False``.
    """
    try:
        client.branches_api.delete_branch(repository=repository, branch=branch)
    except NotFoundException as e:
        if not missing_ok:
            raise e


def list_branches(client: LakeFSClient, repository: str, prefix: str | None = None) -> list[Ref]:
    """
    List branches in a repository.

    Parameters
    ----------
    client: LakeFSClient
        lakeFS client object.
    repository: str
        Name of the repository for which to list branches.
    prefix: str | None
        Return branches prefixed with this value.

    Returns
    -------
    list[Ref]
        A list of qualified branches in the repository.
    """
    return list(depaginate(client.branches_api.list_branches, repository=repository, prefix=prefix))


def create_repository(
    client: LakeFSClient, name: str, storage_namespace: str, exist_ok: bool = True
) -> Repository:
    """
    Create a new repository in the lakeFS file storage system with a specified name and storage namespace.

    Parameters
    ----------
    client: LakeFSClient
        lakeFS client object.
    name: str
        Name of the repository to be created. This must be unique within the lakeFS instance.
    storage_namespace: str
        Storage namespace where the repository data will reside, typically corresponding to a bucket in object storage (e.g., S3 bucket) or a local namespace (e.g. local://<repo_name>).
    exist_ok: bool
        Ignore creation errors if the repository already exists.

    Returns
    -------
    Repository
        Repository object of the lakeFS SDK representing the newly created or existing repository.

    Raises
    ------
    ApiException
        If a repository of the same name already exists and ``exist_ok=False``.

    Notes
    -----
    Attempting to recreate a repository with the same name and storage namespace after previous deletion may lead to issues due to residual data, and is not recommended.
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


def create_tag(client: LakeFSClient, repository: str, ref: str | Commit, tag: str) -> Ref:
    """
    Create a new tag in the specified repository in the lakeFS file storage system.

    Parameters
    ----------
    client: LakeFSClient
        lakeFS client object.
    repository: str
        Name of the repository where the tag will be created.
    ref: str | Commit
        Commit SHA or Commit object of the commit to which the tag will point.
    tag: str
        Name of the tag to be created.

    Raises
    ------
    ApiException
        If a tag of the same name already exists and points to a different ref.

    Returns
    -------
    Ref
        Ref object of the lakeFS SDK representing the tag.
    """

    if isinstance(ref, Commit):
        ref = ref.id
    tag_creation = TagCreation(id=tag, ref=ref)

    try:
        return client.tags_api.create_tag(repository=repository, tag_creation=tag_creation)
    except ApiException as e:
        if e.status == 409:
            target_commit = rev_parse(client=client, repository=repository, ref=ref)
            existing_tag = client.tags_api.get_tag(repository=repository, tag=tag)
            if existing_tag.commit_id == target_commit.id:
                return existing_tag
        raise e


def delete_tag(client: LakeFSClient, repository: str, tag: str | Ref) -> None:
    """
    Delete the specified tag from a repository.

    Parameters
    ----------
    client: LakeFSClient
        lakeFS client object.
    repository: str
        Name of the repository from which the tag will be deleted.
    tag: str | Ref
        Tag to be deleted.
    """
    if isinstance(tag, Ref):
        tag = tag.id
    client.tags_api.delete_tag(repository=repository, tag=tag)


def list_tags(client: LakeFSClient, repository: str) -> list[Ref]:
    """
    List all the tags in the specified repository in the lakeFS file storage system.

    Parameters
    ----------
    client: LakeFSClient
        lakeFS client object.
    repository: str
        Name of the repository from which to list the tags.

    Returns
    -------
    list[Ref]
        Ref objects of the tag in the repository.
    """
    return list(depaginate(client.tags_api.list_tags, repository))


def merge(client: LakeFSClient, repository: str, source_ref: str, target_branch: str) -> None:
    """
    Merges changes from a source reference to a target branch in a specified repository in the lakeFS file storage system.
    If no differences between source_ref and target_branch are found, the merge process is aborted.

    Parameters
    ----------
    client: LakeFSClient
        lakeFS client object.
    repository: str
        Name of the repository where the merge will occur.
    source_ref: str
        Source reference (branch name or ref/commit SHA) from which changes will be merged.
    target_branch: str
        Target branch to which changes will be merged.
    """
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
    """
    Revert the commit on the specified branch to the parent specified by parent_number.

    Parameters
    ----------
    client: LakeFSClient
        lakeFS client object.
    repository: str
        Repository in which the specified branch is located.
    branch: str
        Branch on which the commit should be reverted.
    parent_number: int, optional
        If there are multiple parents to a commit, specify to which parent the commit should be reverted.
        ``parent_number = 1`` (the default)  refers to the first parent commit of the current ``branch`` tip.
    """
    revert_creation = RevertCreation(ref=branch, parent_number=parent_number)
    client.branches_api.revert_branch(
        repository=repository, branch=branch, revert_creation=revert_creation
    )


def reset_branch(client: LakeFSClient, repository: str, branch: str) -> None:
    """
    Reset the specified branch to its head.

    Parameters
    ----------
    client: LakeFSClient
        lakeFS client object.
    repository: str
        Repository in which the specified branch is located.
    branch: str
        Branch to reset.
    """
    reset_creation = ResetCreation(type="reset")
    client.branches_api.reset_branch(
        repository=repository, branch=branch, reset_creation=reset_creation
    )


def rev_parse(
    client: LakeFSClient,
    repository: str,
    ref: str | Commit,
    parent: int = 0,
) -> Commit:
    """
    Resolve a commit reference to the most recent commit or traverses the specified number of parent commits on a branch in a lakeFS repository.

    Parameters
    ----------
    client: LakeFSClient
        lakeFS client object.
    repository: str
        Name of the repository where the commit will be searched.
    ref: str | Commit
        Commit SHA or Commit object (with SHA stored in its ``id`` attribute) to resolve.
    parent: int
        Optionally parse a parent of ``ref`` instead of ``ref`` itself as indicated by the number.
        Must be non-negative. ``parent = 0`` (the default)  refers to ``ref`` itself.

    Returns
    -------
    Commit
        Commit object representing the resolved commit in the lakeFS repository.

    Raises
    ------
    ValueError
        - If ``parent`` is negative.
        - If the specified number of parent commits exceeds the actual number of available parents.
        - If the provided ``ref`` does not match any revision in the specified repository.
    """
    if parent < 0:
        raise ValueError("Parent number cannot be negative")
    try:
        if isinstance(ref, Commit):
            ref = ref.id
        revisions = list(
            depaginate(
                client.refs_api.log_commits, repository, ref, limit=True, amount=2 * (parent + 1)
            )
        )
        if len(revisions) <= parent:
            raise ValueError(
                f"unable to fetch revision {ref}~{parent}: "
                f"ref {ref!r} only has {len(revisions)} parents"
            )
        return revisions[parent]
    except NotFoundException:
        raise ValueError(f"{ref!r} does not match any revision in lakeFS repository {repository!r}")
