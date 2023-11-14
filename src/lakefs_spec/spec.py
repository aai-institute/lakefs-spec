from __future__ import annotations

import io
import logging
import mimetypes
import operator
import os
import urllib.error
import urllib.request
import warnings
from contextlib import contextmanager
from functools import partial
from pathlib import Path
from typing import Any, Callable, Generator, Literal, overload

from fsspec import filesystem
from fsspec.callbacks import Callback, NoOpCallback
from fsspec.spec import AbstractBufferedFile, AbstractFileSystem
from fsspec.transaction import Transaction
from fsspec.utils import stringify_path
from lakefs_sdk import Configuration
from lakefs_sdk.client import LakeFSClient
from lakefs_sdk.exceptions import ApiException, NotFoundException
from lakefs_sdk.models import ObjectCopyCreation, ObjectStatsList, StagingMetadata

from lakefs_spec.client_helpers import commit, create_tag, ensure_branch, merge, revert
from lakefs_spec.config import LakectlConfig
from lakefs_spec.errors import translate_lakefs_error
from lakefs_spec.util import md5_checksum, parse

_DEFAULT_CALLBACK = NoOpCallback()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

EmptyYield = Generator[None, None, None]

_warn_on_fileupload = True


class LakeFSTransaction(Transaction):
    """A lakeFS transaction model capable of versioning operations in between file uploads."""

    def __init__(self, fs: LakeFSFileSystem):
        """
        Initialize a lakeFS transaction. The base class' `file` stack can also contain
        versioning operations.
        """
        super().__init__(fs=fs)
        self.fs: LakeFSFileSystem
        self.files: list[AbstractBufferedFile | Callable[[LakeFSClient], None]]

    def __enter__(self):
        self.start()
        return self

    def commit(
        self, repository: str, branch: str, message: str, metadata: dict[str, str] | None = None
    ) -> None:
        """
        Create a commit on a branch in a repository with a commit message and attached metadata.
        """

        # bind all arguments to the client helper function, and then add it to the file-/callstack.
        op = partial(
            commit, repository=repository, branch=branch, message=message, metadata=metadata
        )
        self.files.append(op)

    def complete(self, commit: bool = True) -> None:
        """
        Finish transaction: Unwind file+versioning op stack via
         1. Committing or discarding in case of a file, and
         2. Conducting versioning operations using the file system's client.

         No operations happen and all files are discarded if `commit` is False,
         which is the case e.g. if an exception happens in the context manager.
        """
        for f in self.files:
            if isinstance(f, AbstractBufferedFile):
                if commit:
                    f.commit()
                else:
                    f.discard()
            else:
                # member is a client helper, with everything but the client bound
                # via `functools.partial`.
                if commit:
                    f(self.fs.client)
        self.files = []
        self.fs._intrans = False

    def create_branch(self, repository: str, branch: str, source_branch: str) -> str:
        op = partial(
            ensure_branch, repository=repository, branch=branch, source_branch=source_branch
        )
        self.files.append(op)
        return branch

    def merge(self, repository: str, source_ref: str, into: str) -> None:
        """Merge a branch into another branch in a repository."""
        op = partial(merge, repository=repository, source_ref=source_ref, target_branch=into)
        self.files.append(op)

    def revert(self, repository: str, branch: str, parent_number: int = 1) -> None:
        """Revert a previous commit on a branch."""
        op = partial(revert, repository=repository, branch=branch, parent_number=parent_number)
        self.files.append(op)

    def tag(self, repository: str, ref: str, tag: str) -> str:
        """Create a tag referencing a commit in a repository."""
        op = partial(create_tag, repository=repository, ref=ref, tag=tag)
        self.files.append(op)
        return tag


class LakeFSFileSystem(AbstractFileSystem):
    """
    lakeFS file system implementation.

    The client is immutable in this implementation, so different users need different
    file systems.
    """

    protocol = "lakefs"

    def __init__(
        self,
        host: str | None = None,
        username: str | None = None,
        password: str | None = None,
        api_key: str | None = None,
        api_key_prefix: str | None = None,
        access_token: str | None = None,
        verify_ssl: bool = True,
        ssl_ca_cert: str | None = None,
        proxy: str | None = None,
        configfile: str = "~/.lakectl.yaml",
        create_branch_ok: bool = True,
        source_branch: str = "main",
        **storage_options: Any,
    ):
        """
        The LakeFS file system constructor.

        Parameters
        ----------
        host: str or None
            The address of your lakeFS instance.
        username: str or None
            The access key name to use in case of access key authentication.
        password: str or None
            The access key secret to use in case of access key authentication.
        api_key: str or None
            The API key to use in case of authentication with an API key.
        api_key_prefix: str or None
            A string prefix to use for the API key in authentication.
        access_token: str or None
            An access token to use in case of access token authentication.
        verify_ssl: bool
            Whether to verify SSL certificates in API interactions. Do not disable in production.
        ssl_ca_cert: str or None
            A custom certificate PEM file to use to verify the peer in SSL connections.
        proxy: str or None
            Proxy address to use when connecting to a lakeFS server.
        create_branch_ok: bool
            Whether to create branches implicitly when not-existing branches are referenced on file uploads.
        source_branch: str
            Source branch set as origin when a new branch is implicitly created.
        storage_options: Any
            Configuration options to pass to the file system's directory cache.
        """
        super().__init__(**storage_options)

        if (p := Path(configfile).expanduser()).exists():
            lakectl_config = LakectlConfig.read(p)
        else:
            # empty config.
            lakectl_config = LakectlConfig()

        configuration = Configuration(
            host=host or os.getenv("LAKEFS_HOST") or lakectl_config.host,
            api_key=api_key or os.getenv("LAKEFS_API_KEY"),
            api_key_prefix=api_key_prefix or os.getenv("LAKEFS_API_KEY_PREFIX"),
            access_token=access_token or os.getenv("LAKEFS_ACCESS_TOKEN"),
            username=username or os.getenv("LAKEFS_USERNAME") or lakectl_config.username,
            password=password or os.getenv("LAKEFS_PASSWORD") or lakectl_config.password,
            ssl_ca_cert=ssl_ca_cert or os.getenv("LAKEFS_SSL_CA_CERT"),
        )
        # proxy address, not part of the constructor
        configuration.proxy = proxy
        # whether to verify SSL certs, not part of the constructor
        configuration.verify_ssl = verify_ssl

        self.client = LakeFSClient(configuration=configuration)
        self.create_branch_ok = create_branch_ok
        self.source_branch = source_branch

    @classmethod
    @overload
    def _strip_protocol(cls, path: str | os.PathLike[str] | Path) -> str:
        ...

    @classmethod
    @overload
    def _strip_protocol(cls, path: list[str | os.PathLike[str] | Path]) -> list[str]:
        ...

    @classmethod
    def _strip_protocol(cls, path):
        """Copied verbatim from the base class, save for the slash rstrip."""
        if isinstance(path, list):
            return [cls._strip_protocol(p) for p in path]
        spath = super()._strip_protocol(path)
        if stringify_path(path).endswith("/"):
            return spath + "/"
        return spath

    @property
    def transaction(self):
        """A context within which files are committed together upon exit

        Requires the file class to implement `.commit()` and `.discard()`
        for the normal and exception cases.
        """
        self._transaction: LakeFSTransaction | None
        if self._transaction is None:
            self._transaction = LakeFSTransaction(self)
        return self._transaction

    def start_transaction(self):
        """Begin write transaction for deferring files, non-context version"""
        self._intrans = True
        self._transaction = LakeFSTransaction(self)
        return self.transaction

    @contextmanager
    def wrapped_api_call(self, message: str | None = None, set_cause: bool = True) -> EmptyYield:
        try:
            yield
        except ApiException as e:
            raise translate_lakefs_error(e, message=message, set_cause=set_cause)

    @contextmanager
    def scope(
        self,
        create_branch_ok: bool | None = None,
        source_branch: str | None = None,
    ) -> EmptyYield:
        """
        A context manager yielding scope in which the lakeFS file system behavior
        is changed from defaults.
        """
        curr_create_branch_ok, curr_source_branch = (
            self.create_branch_ok,
            self.source_branch,
        )
        try:
            if create_branch_ok is not None:
                self.create_branch_ok = create_branch_ok
            if source_branch is not None:
                self.source_branch = source_branch
            yield
        finally:
            self.create_branch_ok = curr_create_branch_ok
            self.source_branch = curr_source_branch

    def checksum(self, path: str) -> str | None:
        try:
            return self.info(path).get("checksum")
        except FileNotFoundError:
            return None

    def exists(self, path: str, **kwargs: Any) -> bool:
        repository, ref, resource = parse(path)

        try:
            self.client.objects_api.head_object(repository, ref, resource, **kwargs)
            return True
        except NotFoundException:
            return False
        except ApiException as e:
            # in case of an error other than "not found", existence cannot be
            # decided, so raise the translated error.
            raise translate_lakefs_error(e)

    def cp_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        if path1 == path2:
            return

        orig_repo, orig_ref, orig_path = parse(path1)
        dest_repo, dest_ref, dest_path = parse(path2)

        if orig_repo != dest_repo:
            raise ValueError(
                "can only copy objects within a repository, but got source "
                f"repository {orig_repo!r} and destination repository "
                f"{dest_repo!r}"
            )

        with self.wrapped_api_call():
            object_copy_creation = ObjectCopyCreation(src_path=orig_path, src_ref=orig_ref)
            self.client.objects_api.copy_object(
                repository=dest_repo,
                branch=dest_ref,
                dest_path=dest_path,
                object_copy_creation=object_copy_creation,
                **kwargs,
            )

    def get_file(
        self,
        rpath: str,
        lpath: str,
        callback: Callback = _DEFAULT_CALLBACK,
        outfile: Any = None,
        precheck: bool = True,
        **kwargs: Any,
    ) -> None:
        if precheck and Path(lpath).exists():
            local_checksum = md5_checksum(lpath, blocksize=self.blocksize)
            remote_checksum = self.info(rpath).get("checksum")
            if local_checksum == remote_checksum:
                logger.info(
                    f"Skipping download of resource {rpath!r} to local path {lpath!r}: "
                    f"Resource {lpath!r} exists and checksums match."
                )
                return

        super().get_file(rpath=rpath, lpath=lpath, callback=callback, outfile=outfile, **kwargs)

    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        path = self._strip_protocol(path)

        # input path is a directory name
        if path.endswith("/"):
            out = self.ls(path, detail=True, **kwargs)
            if not out:
                raise FileNotFoundError(path)

            resource = path.split("/", maxsplit=2)[-1]
            statobj = {
                "name": resource,
                "size": sum(o.get("size", 0) for o in out),
                "type": "directory",
            }
        # input path is a file name
        else:
            with self.wrapped_api_call():
                repository, ref, resource = parse(path)
                res = self.client.objects_api.stat_object(
                    repository=repository, ref=ref, path=resource, **kwargs
                )

            statobj = {
                "checksum": res.checksum,
                "content-type": res.content_type,
                "mtime": res.mtime,
                "name": res.path,
                "size": res.size_bytes,
                "type": "file",
            }

        return statobj

    def ls(self, path: str, detail: bool = True, **kwargs: Any) -> list:
        path = self._strip_protocol(path)
        repository, ref, prefix = parse(path)

        try:
            cache_entry: list[Any] | None = self._ls_from_cache(prefix)
        except FileNotFoundError:
            # we patch files missing from an ls call in the cache entry below,
            # so this should not be an error.
            cache_entry = None

        if cache_entry is not None:
            if not detail:
                return [e["name"] for e in cache_entry]
            return cache_entry

        has_more, after = True, ""
        # stat infos are either the path only (`detail=False`) or a dict full of metadata
        info: list[Any] = []

        with self.wrapped_api_call():
            while has_more:
                res: ObjectStatsList = self.client.objects_api.list_objects(
                    repository, ref, after=after, prefix=prefix, **kwargs
                )
                has_more, after = res.pagination.has_more, res.pagination.next_offset
                for obj in res.results:
                    info.append(
                        {
                            "checksum": obj.checksum,
                            "content-type": obj.content_type,
                            "mtime": obj.mtime,
                            "name": obj.path,
                            "size": obj.size_bytes,
                            "type": "file",
                        }
                    )

        # cache the info if not empty.
        if info:
            # assumes that the returned info is name-sorted.
            pp = self._parent(info[0]["name"])
            if pp in self.dircache:
                # ls info has files not in cache, so we update them in the cache entry.
                cache_entry = self.dircache[pp]
                # extend the entry by the new ls results
                cache_entry.extend(info)
                self.dircache[pp] = sorted(cache_entry, key=operator.itemgetter("name"))
            else:
                self.dircache[pp] = info

        if not detail:
            info = [o["name"] for o in info]

        return info

    def _open(
        self,
        path: str,
        mode: Literal["rb", "wb"] = "rb",
        block_size: int | None = None,
        autocommit: bool = True,
        cache_options: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> LakeFSFile:
        if mode not in {"rb", "wb"}:
            raise NotImplementedError(f"unsupported mode {mode!r}")

        if mode == "wb":
            global _warn_on_fileupload
            if _warn_on_fileupload:
                warnings.warn(
                    f"Calling `{self.__class__.__name__}.open()` in write mode results in unbuffered "
                    "file uploads, because the lakeFS Python client does not support multipart "
                    "uploads. Uploading large files unbuffered can have performance implications.",
                    UserWarning,
                )
                _warn_on_fileupload = False

        return LakeFSFile(
            self,
            path=path,
            mode=mode,
            block_size=block_size or self.blocksize,
            autocommit=autocommit,
            cache_options=cache_options,
            **kwargs,
        )

    def put_file_to_blockstore(
        self,
        lpath: str,
        rpath: str,
        callback: Callback = _DEFAULT_CALLBACK,
        presign: bool = False,
        storage_options: dict[str, Any] | None = None,
    ) -> None:
        repository, branch, resource = parse(rpath)

        staging_location = self.client.staging_api.get_physical_address(
            repository, branch, resource, presign=presign
        )

        if presign:
            remote_url = staging_location.presigned_url
            content_type, _ = mimetypes.guess_type(lpath)
            if content_type is None:
                content_type = "application/octet-stream"
            with open(lpath, "rb") as f:
                headers = {
                    "Content-Type": content_type,
                }
                request = urllib.request.Request(
                    url=remote_url, data=f, headers=headers, method="PUT"
                )
                try:
                    if not remote_url.lower().startswith("http"):
                        raise ValueError("Wrong protocol for remote connection")
                    else:
                        logger.info(f"Begin upload of {lpath}")
                        with urllib.request.urlopen(request):  # nosec [B310:blacklist] # We catch faulty protocols above.
                            logger.info(f"Successfully uploaded {lpath}")
                except urllib.error.HTTPError as e:
                    urllib_http_error_as_lakefs_api_exception = ApiException(
                        status=e.code, reason=e.reason
                    )
                    translate_lakefs_error(error=urllib_http_error_as_lakefs_api_exception)
        else:
            blockstore_type = self.client.config_api.get_config().storage_config.blockstore_type
            # lakeFS blockstore name is "azure", but Azure's fsspec registry entry is "az".
            if blockstore_type == "azure":
                blockstore_type = "az"

            if blockstore_type not in ["s3", "gs", "az"]:
                raise ValueError(
                    f"Blockstore writes are not implemented for blockstore type {blockstore_type!r}"
                )

            remote_url = staging_location.physical_address
            remote = filesystem(blockstore_type, **(storage_options or {}))
            remote.put_file(lpath, remote_url, callback=callback)

        staging_metadata = StagingMetadata(
            staging=staging_location,
            checksum=md5_checksum(lpath, blocksize=self.blocksize),
            size_bytes=os.path.getsize(lpath),
        )
        self.client.staging_api.link_physical_address(
            repository, branch, resource, staging_metadata
        )

    def put_file(
        self,
        lpath: str,
        rpath: str,
        callback: Callback = _DEFAULT_CALLBACK,
        precheck: bool = True,
        use_blockstore: bool = False,
        presign: bool = False,
        storage_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        if precheck:
            remote_checksum = self.checksum(rpath)
            local_checksum = md5_checksum(lpath, blocksize=self.blocksize)
            if local_checksum == remote_checksum:
                logger.info(
                    f"Skipping upload of resource {lpath!r} to remote path {rpath!r}: "
                    f"Resource {rpath!r} exists and checksums match."
                )
                return

        if use_blockstore:
            self.put_file_to_blockstore(
                lpath,
                rpath,
                presign=presign,
                callback=callback,
                storage_options=storage_options,
            )
        else:
            super().put_file(lpath=lpath, rpath=rpath, callback=callback, **kwargs)

    def rm_file(self, path: str) -> None:
        repository, branch, resource = parse(path)

        with self.wrapped_api_call():
            self.client.objects_api.delete_object(
                repository=repository, branch=branch, path=resource
            )


class LakeFSFile(AbstractBufferedFile):
    """lakeFS file implementation. Buffered in reads, unbuffered in writes."""

    def __init__(
        self,
        fs: LakeFSFileSystem,
        path: str,
        mode: Literal["rb", "wb"] = "rb",
        block_size: int | str = "default",
        autocommit: bool = True,
        cache_type: str = "readahead",
        cache_options: dict[str, Any] | None = None,
        size: int | None = None,
        **kwargs: Any,
    ):
        super().__init__(
            fs,
            path,
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_type=cache_type,
            cache_options=cache_options,
            size=size,
            **kwargs,
        )

        self.buffer: io.BytesIO
        if mode == "wb" and self.fs.create_branch_ok:
            repository, branch, resource = parse(path)
            ensure_branch(self.fs.client, repository, branch, self.fs.source_branch)

    def _upload_chunk(self, final: bool = False) -> bool:
        """Commits the file on final chunk via single-shot upload, no-op otherwise."""
        if final and self.autocommit:
            self.commit()
        return not final

    def commit(self):
        """Commit the file via single-shot upload."""
        repository, branch, resource = parse(self.path)

        with self.fs.wrapped_api_call():
            # empty buffer is equivalent to a touch()
            self.buffer.seek(0)
            self.fs.client.objects_api.upload_object(
                repository=repository,
                branch=branch,
                path=resource,
                content=self.buffer.read(),
            )

        self.buffer = io.BytesIO()

    def discard(self):
        self.buffer = io.BytesIO()  # discards the data, but in a type-safe way.

    def flush(self, force: bool = False) -> None:
        """
        Write buffered data to backend store.

        Writes the current buffer, if it is larger than the block-size, or if
        the file is being closed.

        In contrast to the abstract class, this implementation does NOT unload the buffer
        if it is larger than the block size, because the lakeFS server does not support
        multipart uploads.

        Parameters
        ----------
        force: bool
            When closing, write the last block even if it is smaller than
            blocks are allowed to be. Disallows further writing to this file.
        """

        if self.closed:
            raise ValueError("Flush on closed file")
        self.forced: bool
        if force and self.forced:
            raise ValueError("Force flush cannot be called more than once")
        if force:
            self.forced = True

        if self.mode != "wb":
            # no-op to flush on read-mode
            return

        if not force and self.buffer.tell() < self.blocksize:
            # Defer write on small block
            return

        self.offset: int
        if self.offset is None:
            # Initialize an upload
            self.offset = 0

        if self._upload_chunk(final=force) is not False:
            self.offset += self.buffer.seek(0, 2)

    def _fetch_range(self, start: int, end: int) -> bytes:
        repository, ref, resource = parse(self.path)
        with self.fs.wrapped_api_call():
            return self.fs.client.objects_api.get_object(
                repository, ref, resource, range=f"bytes={start}-{end - 1}"
            )
