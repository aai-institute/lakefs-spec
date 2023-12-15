"""
Functionality for extended lakeFS transactions to conduct versioning operations between file uploads.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Generic, TypeVar

import wrapt
from fsspec.spec import AbstractBufferedFile
from fsspec.transaction import Transaction
from lakefs_sdk.client import LakeFSClient
from lakefs_sdk.models import Commit, Ref

from lakefs_spec.client_helpers import commit, create_branch, create_tag, merge, rev_parse, revert

T = TypeVar("T")

if TYPE_CHECKING:
    from lakefs_spec import LakeFSFileSystem

    VersioningOpTuple = tuple[Callable[[LakeFSFileSystem], None], Any]


@dataclass
class Placeholder(Generic[T], wrapt.ObjectProxy):
    """A generic placeholder for a value computed by the lakeFS server in a versioning operation during a transaction."""

    def __init__(self, wrapped: T | None = None):
        super().__init__(wrapped)

    @property
    def available(self) -> bool:
        """Whether the wrapped value is available, i.e. already computed."""
        return self.__wrapped__ is not None

    @property
    def value(self):
        return self.__wrapped__

    @value.setter
    def value(self, val: T) -> None:
        """Fill in the placeholder. Not meant to be called directly except in the completion of the transaction."""
        self.__wrapped__ = val


class LakeFSTransaction(Transaction):
    """
    A lakeFS transaction model capable of versioning operations in between file uploads.

    Parameters
    ----------
    fs: LakeFSFileSystem
        The lakeFS file system associated with the transaction.
    """

    def __init__(self, fs: "LakeFSFileSystem"):
        super().__init__(fs=fs)
        self.fs: "LakeFSFileSystem"
        self.files: deque[AbstractBufferedFile | VersioningOpTuple] = deque(self.files)

    def __enter__(self):
        self.fs._intrans = True
        return self

    def commit(
        self, repository: str, branch: str, message: str, metadata: dict[str, str] | None = None
    ) -> Placeholder[Commit]:
        """
        Create a commit on a branch in a repository with a commit message and attached metadata.

        Parameters
        ----------
        repository: str
            The repository to create the commit in.
        branch: str
            The name of the branch to commit on.
        message: str
            The commit message to attach to the newly created commit.
        metadata: dict[str, str] | None
            Optional metadata to enrich the created commit with (author, e-mail, etc.).

        Returns
        -------
        Placeholder[Commit]
            A placeholder for the commit created by the dispatched ``commit`` API call.
        """
        # bind all arguments to the client helper function, and then add it to the file-/callstack.
        op = partial(
            commit, repository=repository, branch=branch, message=message, metadata=metadata
        )
        p: Placeholder[Commit] = Placeholder()
        self.files.append((op, p))
        # return a placeholder for the commit.
        return p

    def complete(self, commit: bool = True) -> None:
        """
        Finish the transaction by unwinding the file/versioning op stack via

        1. Committing or discarding in case of a file, and
        2. Conducting versioning operations using the file system's client.

        No operations happen and all files are discarded if ``commit == False``,
        which is the case, e.g., if an exception happens in the context manager.

        Parameters
        ----------
        commit: bool
            Whether to conduct operations queued in the transaction.
        """
        while self.files:
            # fsspec base class calls `append` on the file, which means we
            # have to pop from the left to preserve order.
            f = self.files.popleft()
            if isinstance(f, AbstractBufferedFile):
                if commit:
                    f.commit()
                else:
                    f.discard()
            else:
                # client helper + return value case.
                op, retval = f
                if commit:
                    result = op(self.fs.client)
                    # if the transaction member returns a placeholder,
                    # fill it with the result of the client helper.
                    if isinstance(retval, Placeholder):
                        retval.value = result

        self.fs._intrans = False

    def create_branch(
        self, repository: str, name: str, source_branch: str, exist_ok: bool = True
    ) -> str:
        """
        Create a branch ``name`` in a repository, branching off ``source_branch``.

        Parameters
        ----------
        repository: str
            Repository name.
        name: str
            Name of the branch to be created.
        source_branch: str
            Name of the source branch that the new branch is created from.
        exist_ok: bool
            Ignore creation errors if the branch already exists.

        Returns
        -------
        str
            The requested branch name.
        """
        op = partial(
            create_branch,
            repository=repository,
            name=name,
            source_branch=source_branch,
            exist_ok=exist_ok,
        )
        self.files.append((op, name))
        return name

    def merge(self, repository: str, source_ref: str, into: str) -> None:
        """
        Merge a branch into another branch in a repository.

        Parameters
        ----------
        repository: str
            Name of the repository.
        source_ref: str
            Source reference containing the changes to merge. Can be a branch name or partial commit SHA.
        into: str
            Target branch into which the changes will be merged.
        """
        op = partial(merge, repository=repository, source_ref=source_ref, target_branch=into)
        self.files.append((op, None))
        return None

    def revert(self, repository: str, branch: str, parent_number: int = 1) -> None:
        """
        Revert a previous commit on a branch.

        Parameters
        ----------
        repository: str
            Name of the repository.
        branch: str
            Branch on which the commit should be reverted.
        parent_number: int
            If there are multiple parents to a commit, specify to which parent the commit should be reverted.
            ``parent_number = 1`` (the default)  refers to the first parent commit of the current ``branch`` tip.
        """

        op = partial(revert, repository=repository, branch=branch, parent_number=parent_number)
        self.files.append((op, None))
        return None

    def rev_parse(
        self, repository: str, ref: str | Placeholder[Commit], parent: int = 0
    ) -> Placeholder[Commit]:
        """
        Parse a given reference or any of its parents in a repository.

        Parameters
        ----------
        repository: str
            Name of the repository.
        ref: str | Placeholder[Commit]
            Commit SHA or commit placeholder object to resolve.
        parent: int
            Optionally parse a parent of ``ref`` instead of ``ref`` itself as indicated by the number.
            Must be non-negative. ``parent = 0`` (the default)  refers to ``ref`` itself.

        Returns
        -------
        Placeholder[Commit]
            A placeholder for the commit created by the dispatched ``rev_parse`` API call.
        """

        def rev_parse_op(client: LakeFSClient, **kwargs: Any) -> Commit:
            return rev_parse(client, **kwargs)

        p: Placeholder[Commit] = Placeholder()
        op = partial(rev_parse_op, repository=repository, ref=ref, parent=parent)
        self.files.append((op, p))
        return p

    def tag(self, repository: str, ref: str | Placeholder[Commit], tag: str) -> str:
        """
        Create a tag referencing a commit in a repository.

        Parameters
        ----------
        repository: str
            Name of the repository.
        ref: str | Placeholder[Commit]
            Commit SHA or placeholder for a commit object to which the new tag will point.
        tag: str
            Name of the tag to be created.

        Returns
        -------
        str
            The name of the requested tag.
        """

        def tag_op(client: LakeFSClient, **kwargs: Any) -> Ref:
            return create_tag(client, **kwargs)

        self.files.append((partial(tag_op, repository=repository, ref=ref, tag=tag), tag))
        return tag
