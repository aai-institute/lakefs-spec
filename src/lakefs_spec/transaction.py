"""
Functionality for extended lakeFS transactions to conduct versioning operations between file uploads.
"""

from __future__ import annotations

import logging
import random
import string
from collections import deque
from typing import TYPE_CHECKING, TypeVar

import lakefs
from fsspec.transaction import Transaction
from lakefs.branch import Branch, Reference
from lakefs.object import ObjectWriter
from lakefs.reference import Commit, ReferenceType
from lakefs.repository import Repository
from lakefs.tag import Tag

T = TypeVar("T")

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if TYPE_CHECKING:  # pragma: no cover
    from lakefs_spec import LakeFSFileSystem


class LakeFSTransaction(Transaction):
    """
    A lakeFS transaction model capable of versioning operations in between file uploads.

    Creates an ephemeral branch, conducts all uploads and operations on that branch,
    and optionally merges it back into the source branch on success.

    Parameters
    ----------
    fs: LakeFSFileSystem
        The lakeFS file system associated with the transaction.
    repository: str | Repository
        The repository in which to conduct the transaction.
    base_branch: str | Branch
        The branch on which the resulting files should end up.
    automerge: bool
        Automatically merge the ephemeral branch into the base branch after successful
        transaction completion.
    delete: bool
        Delete the ephemeral branch after the transaction.
    """

    def __init__(
        self,
        fs: "LakeFSFileSystem",
        repository: str | Repository,
        base_branch: str | Branch = "main",
        automerge: bool = True,
        delete: bool = True,
    ):
        super().__init__(fs=fs)
        self.fs: "LakeFSFileSystem"
        self.files: deque[ObjectWriter] = deque(self.files)

        if isinstance(repository, str):
            self.repository = repository
        else:
            self.repository = repository.id

        self.base_branch = base_branch
        self.automerge = automerge
        self.delete = delete

        ephem_name = "transaction-" + "".join(random.choices(string.digits, k=6))  # nosec: B311
        self._ephemeral_branch = Branch(self.repository, ephem_name, client=self.fs.client)

    def __enter__(self):
        self._ephemeral_branch.create(self.base_branch, exist_ok=False)
        self.fs._intrans = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.complete(commit=exc_type is None)

        if self.automerge:
            self._ephemeral_branch.merge_into(self.base_branch)
        if self.delete:
            self._ephemeral_branch.delete()

        self.fs._intrans = False
        self.fs._transaction = None

    @property
    def branch(self):
        return self._ephemeral_branch

    def complete(self, commit: bool = True) -> None:
        """
        Finish the transaction by unwinding the file stack.

        The branch will not be merged and all files are discarded if ``commit == False``,
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
            if not commit:
                f.discard()

    def commit(self, message: str, metadata: dict[str, str] | None = None) -> Reference:
        """
        Create a commit on this transaction's ephemeral branch with a commit message
        and attached metadata.

        Parameters
        ----------
        message: str
            The commit message to attach to the newly created commit.
        metadata: dict[str, str] | None
            Optional metadata to enrich the created commit with (author, e-mail, ...).

        Returns
        -------
        Reference
            The created commit.
        """

        diff = list(self.branch.uncommitted())

        if not diff:
            logger.warning(f"No changes to commit on branch {self.branch.id!r}.")
            return self.branch.head

        return self.branch.commit(message, metadata=metadata)

    def merge(self, source_ref: str | Branch, into: str | Branch) -> str:
        """
        Merge a branch into another branch in a repository.

        Parameters
        ----------
        source_ref: str | Branch
            Source reference containing the changes to merge.
            Can be a branch name or partial commit SHA.
        into: str | Branch
            Target branch into which the changes will be merged.

        Returns
        -------
        str
            The created merge commit ID.
        """
        if isinstance(source_ref, Branch):
            b = source_ref
        else:
            b = lakefs.Branch(self.repository, source_ref, client=self.fs.client)

        return b.merge_into(into)

    def revert(self, branch: str | Branch, ref: ReferenceType, parent_number: int = 1) -> None:
        """
        Revert a previous commit on a branch.

        Parameters
        ----------
        branch: str | Branch
            Branch on which the commit should be reverted.
        ref: ReferenceType
            The reference to revert.
        parent_number: int
            If there are multiple parents to a commit, specify to which parent
            the commit should be reverted. ``parent_number = 1`` (the default)
            refers to the first parent commit of the current ``branch`` tip.
        """

        if isinstance(branch, Branch):
            b = branch
        else:
            b = lakefs.Branch(self.repository, branch, client=self.fs.client)

        ref_id = ref if isinstance(ref, str) else ref.id
        b.revert(ref_id, parent_number=parent_number)
        return None

    def rev_parse(self, ref: ReferenceType) -> Commit:
        """
        Parse a given lakeFS reference expression and obtain its corresponding commit.

        Parameters
        ----------
        ref: ReferenceType
            Reference object to resolve, can be a branch, commit SHA, or tag.

        Returns
        -------
        Commit
            The commit referenced by the expression ``ref``.
        """

        ref_id = ref.id if isinstance(ref, Reference) else ref
        reference = lakefs.Reference(self.repository, ref_id, client=self.fs.client)
        return reference.get_commit()

    def tag(self, ref: ReferenceType, tag: str) -> Tag:
        """
        Create a tag referencing a commit in a repository.

        Parameters
        ----------
        ref: ReferenceType
            Commit SHA or placeholder for a reference or commit object
            to which the new tag will point.
        tag: str
            Name of the tag to be created.

        Returns
        -------
        Tag
            The requested tag.
        """

        return lakefs.Tag(self.repository, tag, client=self.fs.client).create(ref)
