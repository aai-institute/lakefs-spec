from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Generic, TypeVar

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
class Placeholder(Generic[T]):
    value: T | None = None

    def available(self):
        return self.value is not None

    def set_value(self, value: T) -> None:
        self.value = value

    def unwrap(self) -> T:
        if self.value is None:
            raise RuntimeError("placeholder unfilled")
        return self.value


def unwrap_placeholders(kwargs: dict[str, Any]) -> dict[str, Any]:
    return {k: v.unwrap() if isinstance(v, Placeholder) else v for k, v in kwargs.items()}


class LakeFSTransaction(Transaction):
    """A lakeFS transaction model capable of versioning operations in between file uploads."""

    def __init__(self, fs: "LakeFSFileSystem"):
        """
        Initialize a lakeFS transaction. The base class' `file` stack can also contain
        versioning operations.
        """
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
        Finish transaction: Unwind file+versioning op stack via
         1. Committing or discarding in case of a file, and
         2. Conducting versioning operations using the file system's client.

         No operations happen and all files are discarded if `commit` is False,
         which is the case e.g. if an exception happens in the context manager.
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
                        retval.set_value(result)

        self.fs._intrans = False

    def create_branch(
        self, repository: str, name: str, source_branch: str, exist_ok: bool = True
    ) -> str:
        """
        Create a branch with the name `name` in a repository, branching off `source_branch`.
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
        """Merge a branch into another branch in a repository."""
        op = partial(merge, repository=repository, source_ref=source_ref, target_branch=into)
        self.files.append((op, None))
        return None

    def revert(self, repository: str, branch: str, parent_number: int = 1) -> None:
        """Revert a previous commit on a branch."""
        op = partial(revert, repository=repository, branch=branch, parent_number=parent_number)
        self.files.append((op, None))
        return None

    def rev_parse(
        self, repository: str, ref: str | Placeholder[Commit], parent: int = 0
    ) -> Placeholder[Commit]:
        """Parse a given reference or any of its parents in a repository."""

        def rev_parse_op(client: LakeFSClient, **kwargs: Any) -> Commit:
            kwargs = unwrap_placeholders(kwargs)
            return rev_parse(client, **kwargs)

        p: Placeholder[Commit] = Placeholder()
        op = partial(rev_parse_op, repository=repository, ref=ref, parent=parent)
        self.files.append((op, p))
        return p

    def tag(self, repository: str, ref: str | Placeholder[Commit], tag: str) -> str:
        """Create a tag referencing a commit in a repository."""

        def tag_op(client: LakeFSClient, **kwargs: Any) -> Ref:
            kwargs = unwrap_placeholders(kwargs)
            return create_tag(client, **kwargs)

        self.files.append((partial(tag_op, repository=repository, ref=ref, tag=tag), tag))
        return tag
