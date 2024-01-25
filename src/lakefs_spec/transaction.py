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

    Creates an ephemeral branch, grouping all file uploads and versioning operations on that branch,
    and optionally merges them back on success.

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

    def commit(
        self,
        message: str,
        metadata: dict[str, str] | None = None,
    ) -> Reference:
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
        ephem = self.ephemeral_branch

        diff = list(ephem.uncommitted())

        if not diff:
            logger.warning(f"No changes to commit on branch {ephem.id!r}.")
            return ephem.head

        return ephem.commit(message, metadata=metadata)

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
            if isinstance(f, ObjectWriter):
                if not commit:
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

    def create_branch(
        self, repository: str | Repository, name: str, source: str | Branch, exist_ok: bool = True
    ) -> str:
        """
        Create a branch ``name`` in a repository, branching off ``source_branch``.

        Parameters
        ----------
        repository: str | Repository
            Repository name.
        name: str
            Name of the branch to be created.
        source: str | Branch
            Name of the branch (or branch object) that the new branch is created from.
        exist_ok: bool
            Ignore creation errors if the branch already exists.

        Returns
        -------
        str
            The requested branch name.
        """

        def create_branch_op(
            client: Client,
            repo_: str | Repository,
            branch_: str,
            source_: str | Branch,
            exist_ok_: bool,
        ) -> Branch:
            repo_id = repo_.id if isinstance(repo_, Repository) else repo_
            return lakefs.Branch(repo_id, branch_, client=client).create(
                source_, exist_ok=exist_ok_
            )

        op = partial(
            create_branch_op,
            repo_=repository,
            branch_=name,
            source_=source,
            exist_ok_=exist_ok,
        )
        self.files.append((op, name))
        return name

    def merge(
        self, repository: str | Repository, source_ref: str | Branch, into: str | Branch
    ) -> None:
        """
        Merge a branch into another branch in a repository.

        Parameters
        ----------
        repository: str | Repository
            Name of the repository.
        source_ref: str | Branch
            Source reference containing the changes to merge. Can be a branch name or partial commit SHA.
        into: str | Branch
            Target branch into which the changes will be merged.
        """

        def merge_op(
            client: Client, repo_: str | Branch, ref_: str | Branch, into_: str | Branch
        ) -> None:
            repo_id = repo_.id if isinstance(repo_, Repository) else repo_
            ref_id = ref_.id if isinstance(ref_, Branch) else ref_
            lakefs.Branch(repo_id, ref_id, client=client).merge_into(into_)

        op = partial(merge_op, repo_=repository, ref_=source_ref, into_=into)
        self.files.append((op, None))
        return None

    def revert(
        self, repository: str | Repository, branch: str | Branch, parent_number: int = 1
    ) -> None:
        """
        Revert a previous commit on a branch.

        Parameters
        ----------
        repository: str | Repository
            Name of the repository.
        branch: str | Branch
            Branch on which the commit should be reverted.
        parent_number: int
            If there are multiple parents to a commit, specify to which parent the commit should be reverted.
            ``parent_number = 1`` (the default)  refers to the first parent commit of the current ``branch`` tip.
        """

        def revert_op(
            client: Client, repo_: str | Repository, branch_: str | Branch, parent_: int
        ) -> None:
            repo_id = repo_.id if isinstance(repo_, Repository) else repo_
            branch_id = branch_.id if isinstance(branch_, Branch) else branch_
            lakefs.Branch(repo_id, branch_id, client=client).revert(branch_id, parent_)

        op = partial(revert_op, repo_=repository, branch_=branch, parent_=parent_number)
        self.files.append((op, None))
        return None

    def rev_parse(
        self, repository: str | Repository, ref: ReferenceType, parent: int = 0
    ) -> Placeholder[Commit]:
        """
        Parse a given reference or any of its parents in a repository.

        Parameters
        ----------
        repository: str | Repository
            Name of the repository.
        ref: ReferenceType
            Reference object to resolve, can be a branch, commit SHA, or tag.
        parent: int
            Optionally parse a parent of ``ref`` instead of ``ref`` itself as indicated by the number.
            Must be non-negative. ``parent = 0`` (the default)  refers to ``ref`` itself.

        Returns
        -------
        Placeholder[Commit]
            A placeholder for the commit created by the dispatched ``rev_parse`` operation.
        """

        def rev_parse_op(
            client: Client, repo_: str | Repository, ref_: ReferenceType, parent_: int
        ) -> Commit:
            repo_id = repo_.id if isinstance(repo_, Repository) else repo_
            ref_id = ref_.id if isinstance(ref_, Reference) else ref_

            commits = list(lakefs.Reference(repo_id, ref_id, client=client).log(parent_ + 1))
            if len(commits) <= parent:
                raise ValueError(
                    f"unable to fetch revision {ref_id}~{parent_}: "
                    f"ref {ref_id!r} only has {len(commits)} parents"
                )
            return commits[parent_]

        p: Placeholder[Commit] = Placeholder()
        op = partial(rev_parse_op, repo_=repository, ref_=ref, parent_=parent)
        self.files.append((op, p))
        return p

    def tag(self, repository: str | Repository, ref: ReferenceType, tag: str) -> str:
        """
        Create a tag referencing a commit in a repository.

        Parameters
        ----------
        repository: str | Repository
            Name of the repository.
        ref: ReferenceType
            Commit SHA or placeholder for a reference or commit object to which the new tag will point.
        tag: str
            Name of the tag to be created.

        Returns
        -------
        str
            The name of the requested tag.
        """

        def tag_op(client: Client, repo_: str | Repository, ref_: ReferenceType, tag_: str) -> Tag:
            repo_id = repo_.id if isinstance(repo_, Repository) else repo_
            ref_id = ref_.id if isinstance(ref_, Commit) else ref_
            return lakefs.Tag(repo_id, tag_, client=client).create(ref_id)

        self.files.append((partial(tag_op, repo_=repository, ref_=ref, tag_=tag), tag))
        return tag
