import logging
import sys

from lakefs_client.client import LakeFSClient
from lakefs_client.model.commit_creation import CommitCreation

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))


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
