import logging

from lakefs_client.client import LakeFSClient
from lakefs_client.exceptions import NotFoundException
from lakefs_client.model.commit_creation import CommitCreation
from lakefs_client.model.revert_creation import RevertCreation
from lakefs_client.model.tag_creation import TagCreation

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def commit(
    client: LakeFSClient,
    repository: str,
    branch: str,
    message: str,
    metadata: dict[str, str] | None = None,
) -> None:
    diff = client.branches_api.diff_branch(repository=repository, branch=branch)

    if not diff.results:
        logger.warning(f"No changes to commit on branch {branch!r}, aborting commit.")
        return

    commit_creation = CommitCreation(message=message, metadata=metadata or {})

    client.commits_api.commit(repository=repository, branch=branch, commit_creation=commit_creation)


def create_tag(client: LakeFSClient, repository: str, ref: str, tag: str) -> None:
    tag_creation = TagCreation(tag, ref=ref)
    client.tags_api.create_tag(repository=repository, tag_creation=tag_creation)


def get_tags(client: LakeFSClient, repository: str) -> dict:
    return client.tags_api.list_tags(repository=repository)


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

    Args:
        client (LakeFSClient): The client to interact with.
        repository (str): Repository in which the specified branch is located.
        branch (str): Branch on which the commit should be reverted.
        parent_number (int, optional): If there are multiple parents to a commit, specify to which parent the commit should be reverted. Index starts at 1. Defaults to 1.
    """
    revert_creation = RevertCreation(ref=branch, parent_number=parent_number)
    client.branches_api.revert_branch(
        repository=repository, branch=branch, revert_creation=revert_creation
    )


def rev_parse(
    client: LakeFSClient,
    repository: str,
    ref: str,
    parent: int = 0,
) -> str:
    if parent < 0:
        raise ValueError(f"Parent cannot be negative, got {parent}")
    try:
        revisions = client.refs_api.log_commits(
            repository=repository, ref=ref, limit=True, amount=2 * (parent + 1)
        ).results
        if len(revisions) <= parent:
            raise ValueError(
                f"cannot fetch revision {ref}~{parent}: {ref} only has {len(revisions)} parents"
            )
        return revisions[parent].id
    except NotFoundException:
        raise RuntimeError(
            f"{ref!r} does not match any revision in lakeFS repository {repository!r}"
        )
