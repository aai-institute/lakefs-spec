"""
Functionality for extended lakeFS transactions to conduct versioning operations between file uploads.
"""

import logging
import random
import string
import warnings
from collections import deque
from typing import TYPE_CHECKING, Literal, TypeVar

import lakefs
from fsspec.transaction import Transaction
from lakefs.branch import Branch, Reference
from lakefs.client import Client
from lakefs.exceptions import ServerException
from lakefs.object import ObjectWriter
from lakefs.reference import Commit, ReferenceType
from lakefs.repository import Repository
from lakefs.tag import Tag

T = TypeVar("T")

logger = logging.getLogger("lakefs-spec")

if TYPE_CHECKING:  # pragma: no cover
    from lakefs_spec import LakeFSFileSystem


def _ensurebranch(b: str | Branch, repository: str, client: Client) -> Branch:
    if isinstance(b, str):
        return Branch(repository, b, client=client)
    return b


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
        self.fs: LakeFSFileSystem
        self.files: deque[ObjectWriter] = deque(self.files)

        self.repository: str | None = None
        self.base_branch: Branch | None = None
        self.automerge: bool = False
        self.delete: Literal["onsuccess", "always", "never"] = "onsuccess"
        self.squash: bool = False
        self._ephemeral_branch: Branch | None = None

    def __call__(
        self,
        repository: str | Repository,
        base_branch: str | Branch = "main",
        branch_name: str | None = None,
        automerge: bool = True,
        delete: Literal["onsuccess", "always", "never"] = "onsuccess",
        squash: bool = False,
    ) -> "LakeFSTransaction":
        """
        Creates an ephemeral branch, conducts all uploads and operations on that branch,
        and optionally merges it back into the source branch.

        repository: str | Repository
            The repository in which to conduct the transaction.
        base_branch: str | Branch
            The branch on which the transaction operations should be based.
        automerge: bool
            Automatically merge the ephemeral branch into the base branch after successful
            transaction completion.
        delete: Literal["onsuccess", "always", "never"]
            Cleanup policy / deletion handling for the ephemeral branch after the transaction.

            If ``"onsuccess"``, the branch is deleted if the transaction succeeded,
            or left over if an error occurred.

            If ``"always"``, the ephemeral branch is always deleted after transaction regardless of success
            or failure.

            If ``"never"``, the transaction branch is always left in the repository.
        squash: bool
            Optionally squash-merges the transaction branch into the base branch.
        """

        if isinstance(repository, str):
            self.repository = repository
        else:
            self.repository = repository.id

        repo = lakefs.Repository(self.repository, client=self.fs.client)
        try:
            _ = repo.metadata
        except ServerException:
            raise ValueError(f"repository {self.repository!r} does not exist") from None

        # base branch needs to be a lakefs.Branch, since it is being diffed
        # with the ephemeral branch in __exit__.
        self.base_branch = _ensurebranch(base_branch, self.repository, self.fs.client)

        self.automerge = automerge
        self.delete = delete
        self.squash = squash

        ephem_name = branch_name or "transaction-" + "".join(random.choices(string.digits, k=6))  # noqa: S311
        self._ephemeral_branch = Branch(self.repository, ephem_name, client=self.fs.client)
        return self

    def __enter__(self):
        logger.debug(
            f"Creating ephemeral branch {self._ephemeral_branch.id!r} "
            f"from branch {self.base_branch.id!r}."
        )
        self._ephemeral_branch.create(self.base_branch, exist_ok=False)
        self.fs._intrans = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        success = exc_type is None
        while self.files:
            # fsspec base class calls `append` on the file, which means we
            # have to pop from the left to preserve order.
            f = self.files.popleft()
            if not success:
                f.discard()

        self.fs._intrans = False
        self.fs._transaction = None

        if any(self._ephemeral_branch.uncommitted()):
            msg = f"Finished transaction on branch {self._ephemeral_branch.id!r} with uncommitted changes."
            if self.delete != "never":
                msg += " Objects added but not committed are lost."
            warnings.warn(msg)

        if success and self.automerge:
            if any(self.base_branch.diff(self._ephemeral_branch)):
                self._ephemeral_branch.merge_into(self.base_branch, squash_merge=self.squash)
        if self.delete == "always" or (success and self.delete == "onsuccess"):
            self._ephemeral_branch.delete()

    @property
    def branch(self):
        return self._ephemeral_branch

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

    def merge(self, source_ref: str | Branch, into: str | Branch, squash: bool = False) -> Commit:
        """
        Merge a branch into another branch in a repository.

        In case the branch contains no changes relevant to the target branch,
        no merge happens, and the tip of the target branch is returned instead.

        Parameters
        ----------
        source_ref: str | Branch
            Source reference containing the changes to merge.
            Can be a branch name or partial commit SHA.
        into: str | Branch
            Target branch into which the changes will be merged.
        squash: bool
            Optionally squash-merges the source reference into the target branch.

        Returns
        -------
        Commit
            Either the created merge commit, or the head commit of the target branch.
        """
        source = _ensurebranch(source_ref, self.repository, self.fs.client)
        dest = _ensurebranch(into, self.repository, self.fs.client)

        if any(dest.diff(source)):
            source.merge_into(dest, squash_merge=squash)
        return dest.head.get_commit()

    def revert(self, branch: str | Branch, ref: ReferenceType, parent_number: int = 1) -> Commit:
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

        Returns
        -------
        Commit
            The created revert commit.
        """

        b = _ensurebranch(branch, self.repository, self.fs.client)

        ref_id = ref if isinstance(ref, str) else ref.id
        b.revert(ref_id, parent_number=parent_number)
        return b.head.get_commit()

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

    def tag(self, ref: ReferenceType, name: str) -> Tag:
        """
        Create a tag referencing a commit in a repository.

        Parameters
        ----------
        ref: ReferenceType
            Commit SHA or placeholder for a reference or commit object
            to which the new tag will point.
        name: str
            Name of the tag to be created.

        Returns
        -------
        Tag
            The requested tag.
        """

        return lakefs.Tag(self.repository, name, client=self.fs.client).create(ref)
