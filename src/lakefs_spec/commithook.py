from enum import StrEnum, auto
from typing import Callable, NamedTuple

from lakefs_client.models import CommitCreation, DiffList


class FSEvent(StrEnum):
    PUT = auto()
    RM = auto()


class HookContext(NamedTuple):
    repository: str
    branch: str
    resource: str
    diff: DiffList


CommitHook = Callable[[FSEvent, HookContext], CommitCreation]
"""
A hook to execute before a lakeFS commit is created during stateful file operations such as uploads or deletes.
Input arguments are fsspec event name (e.g. ``put``, ``rm``), and a context object containing repository, branch name, (remote) resource name, and the diff to the current head of the given branch.
The output needs to be a ``lakefs_client.models.CommitCreation`` object to pass to the ``LakeFSClient.commits.commit`` API.
"""


def Default(fs_event: FSEvent, ctx: HookContext) -> CommitCreation:
    """
    The most basic commithook, emitting only the message of which path has
    been modified.
    """

    if fs_event == FSEvent.PUT:
        action = "Add"
    elif fs_event == FSEvent.RM:
        action = "Remove"
    else:
        raise ValueError(f"unexpected file system event '{str(fs_event)}'")

    message = f"{action} file {ctx.resource}"

    return CommitCreation(message=message)
