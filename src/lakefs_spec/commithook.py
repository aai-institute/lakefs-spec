from typing import Callable

from lakefs_client.models import CommitCreation

CommitHook = Callable[[str, str], CommitCreation]
"""
A hook to execute before a lakeFS commit is created during stateful file operations such as
uploads or deletes. Input arguments are fsspec event name (e.g. ``put``, ``rm``) and rpath,
the output needs to be a ``lakefs_client.models.CommitCreation`` object to pass to the
``LakeFSClient.commits.commit`` API.
"""


def Default(event: str, rpath: str) -> CommitCreation:
    """
    The most basic commithook, emitting only the message of which path has
    been modified.
    """

    if event in ("put", "put_file"):
        action = "Add"
    elif event in ("rm", "rm_file"):
        action = "Remove"
    else:
        raise ValueError(f"unknown file event {event!r}")

    message = f"""{action} file {rpath}"""

    return CommitCreation(message=message)
