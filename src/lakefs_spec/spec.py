"""
Core interface definitions for file system interaction with lakeFS from Python,
namely the ``LakeFSFileSystem`` and ``LakeFSFile`` classes.
"""

from __future__ import annotations

import errno
import io
import logging
import mimetypes
import operator
import os
import urllib.error
import urllib.request
from contextlib import contextmanager
from functools import cached_property
from pathlib import Path
from typing import Any, Generator, Iterable, Literal, cast, overload

import fsspec.callbacks
from fsspec import filesystem
from fsspec.callbacks import _DEFAULT_CALLBACK
from fsspec.spec import AbstractBufferedFile, AbstractFileSystem
from fsspec.utils import stringify_path
from lakefs_sdk import Configuration
from lakefs_sdk.client import LakeFSClient
from lakefs_sdk.exceptions import ApiException, NotFoundException
from lakefs_sdk.models import ObjectCopyCreation, ObjectStats, StagingMetadata

from lakefs_spec.client_helpers import create_branch
from lakefs_spec.config import LakectlConfig
from lakefs_spec.errors import translate_lakefs_error
from lakefs_spec.transaction import LakeFSTransaction
from lakefs_spec.util import depaginate, md5_checksum, parse

logger = logging.getLogger(__name__)


class LakeFSFileSystem(AbstractFileSystem):
    """
    lakeFS file system implementation.

    Instances of this class are cached based on their constructor arguments.

    For more information, see the fsspec documentation <https://filesystem-spec.readthedocs.io/en/latest/features.html#instance-caching>.

    Parameters
    ----------
    host: str | None
        The address of your lakeFS instance.
    username: str | None
        The access key name to use in case of access key authentication.
    password: str | None
        The access key secret to use in case of access key authentication.
    api_key: str | None
        The API key to use in case of authentication with an API key.
    api_key_prefix: str | None
        A string prefix to use for the API key in authentication.
    access_token: str | None
        An access token to use in case of access token authentication.
    verify_ssl: bool
        Whether to verify SSL certificates in API interactions. Do not disable in production.
    ssl_ca_cert: str | None
        A custom certificate PEM file to use to verify the peer in SSL connections.
    proxy: str | None
        Proxy address to use when connecting to a lakeFS server.
    configfile: str
        ``lakectl`` YAML configuration file to read credentials from.
    create_branch_ok: bool
        Whether to create branches implicitly when not-existing branches are referenced on file uploads.
    source_branch: str
        Source branch set as origin when a new branch is implicitly created.
    **storage_options: Any
        Configuration options to pass to the file system's directory cache.
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

    @cached_property
    def _lakefs_server_version(self):
        with self.wrapped_api_call():
            version_string = self.client.config_api.get_config().version_config.version
            return tuple(int(t) for t in version_string.split("."))

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
        """
        A context manager within which file uploads and versioning operations are deferred to a
        queue, and carried out during when exiting the context.

        Requires the file class to implement ``.commit()`` and ``.discard()`` for the normal and exception cases.
        """
        self._transaction: LakeFSTransaction | None
        if self._transaction is None:
            self._transaction = LakeFSTransaction(self)
        return self._transaction

    def start_transaction(self):
        """
        Prepare a lakeFS file system transaction without entering the transaction context yet.
        """
        self._intrans = True
        self._transaction = LakeFSTransaction(self)
        return self.transaction

    @contextmanager
    def wrapped_api_call(
        self, rpath: str | None = None, message: str | None = None, set_cause: bool = True
    ) -> Generator[None, None, None]:
        """
        A context manager to wrap lakeFS API calls, translating any API errors to Python-native OS errors.

        Meant for internal use.

        Parameters
        ----------
        rpath: str | None
            The remote path involved in the requested API call.
        message: str | None
            A custom error message to emit instead of parsing the API error response.
        set_cause: bool
            Whether to include the original lakeFS API error in the resulting traceback.

        Yields
        ------
        None
            An empty generator, to be used as a context manager.

        Raises
        ------
        OSError
            Translated error from the lakeFS API call, if any.
        """
        try:
            yield
        except ApiException as e:
            raise translate_lakefs_error(e, rpath=rpath, message=message, set_cause=set_cause)

    def checksum(self, path: str | os.PathLike[str]) -> str | None:
        """
        Get a remote lakeFS file object's checksum.

        This is usually its MD5 hash, unless another hash function was used on upload.

        Parameters
        ----------
        path: str | os.PathLike[str]
            The remote path to look up the lakeFS checksum for. Must point to a single file object.

        Returns
        -------
        str | None
            The remote file's checksum, or ``None`` if ``path`` points to a directory or does not exist.
        """
        path = stringify_path(path)
        try:
            return self.info(path).get("checksum")
        except FileNotFoundError:
            return None

    def exists(self, path: str | os.PathLike[str], **kwargs: Any) -> bool:
        """
        Check existence of a remote path in a lakeFS repository.

        Input paths can either be files or directories.

        Parameters
        ----------
        path: str | os.PathLike[str]
            The remote path whose existence to check. Must be a fully qualified lakeFS URI.
        **kwargs: Any
            Additional keyword arguments to pass to ``LakeFSClient.objects_api.head_object()``.

        Returns
        -------
        bool
            ``True`` if the requested path exists, ``False`` if it does not.

        Raises
        ------
        PermissionError
            If the user does not have sufficient permissions to query object existence.
        """
        path = stringify_path(path)
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

    def cp_file(
        self, path1: str | os.PathLike[str], path2: str | os.PathLike[str], **kwargs: Any
    ) -> None:
        """
        Copy a single file from one remote location to another in lakeFS.

        Parameters
        ----------
        path1: str | os.PathLike[str]
            The remote file location to be copied.
        path2: str | os.PathLike[str]
            The (remote) target location to which to copy the file.
        **kwargs: Any
            Additional keyword arguments to pass to ``LakeFSClient.objects_api.copy_object()``.

        Raises
        ------
        ValueError
            When attempting to copy objects between repositories.
        """
        path1 = stringify_path(path1)
        path2 = stringify_path(path2)
        if path1 == path2:
            return

        orig_repo, orig_ref, orig_path = parse(path1)
        dest_repo, dest_ref, dest_path = parse(path2)

        if orig_repo != dest_repo:
            raise ValueError(
                "can only copy objects within a repository, but got source "
                f"repository {orig_repo!r} and destination repository {dest_repo!r}"
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
        rpath: str | os.PathLike[str],
        lpath: str | os.PathLike[str],
        callback: fsspec.callbacks.Callback = _DEFAULT_CALLBACK,
        outfile: Any = None,
        precheck: bool = True,
        **kwargs: Any,
    ) -> None:
        """
        Download a single file from a remote lakeFS server to local storage.

        Parameters
        ----------
        rpath: str | os.PathLike[str]
            The remote path to download to local storage. Must be a fully qualified lakeFS URI, and point to a single file.
        lpath: str | os.PathLike[str]
            The local path on disk to save the downloaded file to.
        callback: fsspec.callbacks.Callback
            An fsspec callback to use during the operation. Can be used to report download progress.
        outfile: Any
            A file-like object to save the downloaded content to. Can be used in place of ``lpath``.
        precheck: bool
            Check if ``lpath`` already exists and compare its checksum with that of ``rpath``, skipping the download if they match.
        **kwargs: Any
            Additional keyword arguments passed to ``AbstractFileSystem.open()``.
        """
        rpath = stringify_path(rpath)
        lpath = stringify_path(lpath)
        lp = Path(lpath)
        if precheck and lp.exists() and lp.is_file():
            local_checksum = md5_checksum(lpath, blocksize=self.blocksize)
            remote_checksum = self.info(rpath).get("checksum")
            if local_checksum == remote_checksum:
                logger.info(
                    f"Skipping download of resource {rpath!r} to local path {lpath!r}: "
                    f"Resource {lpath!r} exists and checksums match."
                )
                return

        with self.wrapped_api_call(rpath=rpath):
            super().get_file(rpath, lpath, callback=callback, outfile=outfile, **kwargs)

    def info(self, path: str | os.PathLike[str], **kwargs: Any) -> dict[str, Any]:
        """
        Query a remote lakeFS object's metadata.

        Parameters
        ----------
        path: str | os.PathLike[str]
            The object for which to obtain metadata. Must be a fully qualified lakeFS URI, can either point to a file or a directory.
        **kwargs: Any
            Additional keyword arguments to pass to either ``LakeFSClient.objects_api.stat_object()``
            (if ``path`` points to a file) or ``LakeFSClient.objects_api.list_objects()`` (if ``path`` points to a directory).

        Returns
        -------
        dict[str, Any]
            A dictionary containing metadata on the object, including its full remote path and object type (file or directory).

        Raises
        ------
        FileNotFoundError
            If the ``path`` refers to a non-file path that does not exist in the repository.
        """
        path = stringify_path(path)
        repository, ref, resource = parse(path)
        # first, try with `stat_object` in case of a file.
        # the condition below checks edge cases of resources that cannot be files.
        if resource and not resource.endswith("/"):
            try:
                # the set of keyword arguments allowed in `list_objects` is a
                # superset of the keyword arguments for `stat_object`.
                # Ensure that only admissible keyword arguments are actually
                # passed to `stat_object`.
                stat_keywords = ["presign", "user_metadata"]
                stat_kwargs = {k: v for k, v in kwargs.items() if k in stat_keywords}

                res = self.client.objects_api.stat_object(
                    repository=repository, ref=ref, path=resource, **stat_kwargs
                )
                return {
                    "checksum": res.checksum,
                    "content-type": res.content_type,
                    "mtime": res.mtime,
                    "name": f"{repository}/{ref}/{res.path}",
                    "size": res.size_bytes,
                    "type": "file",
                }
            except NotFoundException:
                # fall through, retry with `ls` if it's a directory.
                pass
            except ApiException as e:
                raise translate_lakefs_error(e, rpath=path)

        out = self.ls(path, detail=True, recursive=True, **kwargs)
        if not out:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)

        return {
            "name": path.rstrip("/"),
            "size": sum(o.get("size") or 0 for o in out),
            "type": "directory",
        }

    def _update_dircache(self, info: list) -> None:
        """Update logic for dircache (optionally recursive) based on lakeFS API response"""
        parents = {self._parent(i["name"]) for i in info}
        for pp in parents:
            # subset of info entries which are direct descendants of `parent`
            dir_info = [i for i in info if self._parent(i["name"].rstrip("/")) == pp]
            if pp not in self.dircache:
                self.dircache[pp] = dir_info
                continue

            # Merge existing dircache entry with updated listing, which contains either:
            # - files not present in the cache yet
            # - a fresh listing (if `refresh=True`)

            cache_entry = self.dircache[pp][:]

            old_names = {e["name"] for e in cache_entry}
            new_names = {e["name"] for e in dir_info}

            to_remove = old_names - new_names
            to_update = old_names.intersection(new_names)

            # Remove all entries no longer present in the current listing
            cache_entry = [e for e in cache_entry if e["name"] not in to_remove]

            # Overwrite existing entries in the cache with its updated values
            for name in to_update:
                old_idx = next(idx for idx, e in enumerate(cache_entry) if e["name"] == name)
                new_entry = next(e for e in info if e["name"] == name)

                cache_entry[old_idx] = new_entry
                dir_info.remove(new_entry)

            # Add the remaining (new) entries to the cache
            cache_entry.extend(dir_info)
            self.dircache[pp] = sorted(cache_entry, key=operator.itemgetter("name"))

    def _ls_from_cache(self, path: str, recursive: bool = False) -> list[dict[str, Any]] | None:
        """Override of ``AbstractFileSystem._ls_from_cache`` with support for recursive listings."""
        if not recursive:
            return super()._ls_from_cache(path)

        result = None
        for key, files in self.dircache.items():
            if not (key.startswith(path) or path == key + "/"):
                continue
            if result is None:
                result = []
            result.extend(files)
        if not result:
            return result
        return sorted(result, key=operator.itemgetter("name"))

    @overload
    def ls(
        self,
        path: str | os.PathLike[str],
        detail: Literal[True] = ...,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        ...

    @overload
    def ls(
        self,
        path: str | os.PathLike[str],
        detail: Literal[False],
        **kwargs: Any,
    ) -> list[str]:
        ...

    @overload
    def ls(
        self,
        path: str | os.PathLike[str],
        detail: bool = True,
        **kwargs: Any,
    ) -> list[str] | list[dict[str, Any]]:
        ...

    def ls(
        self,
        path: str | os.PathLike[str],
        detail: bool = True,
        **kwargs: Any,
    ) -> list[str] | list[dict[str, Any]]:
        """
        List all available objects under a given path in lakeFS.

        Parameters
        ----------
        path: str | os.PathLike[str]
            The path under which to list objects. Must be a fully qualified lakeFS URI.
            Can also point to a file, in which case the file's metadata will be returned.
        detail: bool
            Whether to obtain all metadata on the requested objects or just their names.
        **kwargs: Any
            Additional keyword arguments to pass to ``LakeFSClient.objects_api.list_objects()``.

            In particular:
                `refresh: bool`: whether to skip the directory listing cache,
                `recursive: bool`: whether to list subdirectory contents recursively

        Returns
        -------
        list[str] | list[dict[str, Any]]
            A list of all objects' metadata under the given remote path if ``detail=True``, or alternatively only their names if ``detail=False``.
        """

        def _api_path_type_to_info(path_type: str) -> Literal["file", "directory"]:
            """Convert ``list_objects()`` API response field ``path_type`` to ``info.type``."""
            if path_type == "object":
                return "file"
            elif path_type == "common_prefix":
                return "directory"
            else:
                raise ValueError(f"unexpected path type {path_type!r}")

        path = cast(str, stringify_path(path))
        repository, ref, prefix = parse(path)

        recursive = kwargs.pop("recursive", False)

        # Try lookup in dircache unless explicitly disabled by `refresh=True` kwarg
        use_dircache = not kwargs.pop("refresh", False)

        if use_dircache:
            cache_entry: list[Any] | None = None
            try:
                cache_entry = self._ls_from_cache(path, recursive=recursive)
            except FileNotFoundError:
                # we patch files missing from an ls call in the cache entry below,
                # so this should not be an error.
                pass

            if cache_entry is not None:
                if not detail:
                    return [e["name"] for e in cache_entry]
                return cache_entry[:]

        kwargs["prefix"] = prefix

        info = []
        # stat infos are either the path only (`detail=False`) or a dict full of metadata
        with self.wrapped_api_call(rpath=path):
            delimiter = "" if recursive else "/"
            objects = depaginate(
                self.client.objects_api.list_objects,
                repository,
                ref,
                delimiter=delimiter,
                **kwargs,
            )
            for obj in cast(Iterable[ObjectStats], objects):
                info.append(
                    {
                        "checksum": obj.checksum,
                        "content-type": obj.content_type,
                        "mtime": obj.mtime,
                        "name": f"{repository}/{ref}/{obj.path}",
                        "size": obj.size_bytes,
                        "type": _api_path_type_to_info(obj.path_type),
                    }
                )

        # Retry the API call with appended slash if the current result
        # is just a single directory entry only (not its contents).
        # This is useful to allow `ls("repo/branch/dir")` calls without
        # a trailing slash.
        if len(info) == 1 and info[0]["type"] == "directory":
            return self.ls(
                path + "/",
                detail=detail,
                **kwargs | {"refresh": not use_dircache, "recursive": recursive},
            )

        if recursive:
            # To make recursive ls behave identical to the non-recursive case,
            # add back virtual `directory` entries, which are only returned by
            # the lakeFS API when querying non-recursively.
            here = self._strip_protocol(path).rstrip("/")
            subdirs = {parent for o in info if (parent := self._parent(o["name"])) != here}
            for subdir in subdirs:
                info.append(
                    {
                        "type": "directory",
                        "name": subdir + "/",
                        "size": 0,
                    }
                )

        if info:
            self._update_dircache(info[:])

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
        """
        Dispatch a lakeFS file (local buffer on disk) for the given remote path for up- or downloads depending on ``mode``.

        Internal only, called by ``AbstractFileSystem.open()``.

        Parameters
        ----------
        path: str
            The remote path for which to open a local ``LakeFSFile``. Must be a fully qualified lakeFS URI.
        mode: Literal["rb", "wb"]
            The file mode indicating its purpose. Use ``rb`` for downloads from lakeFS, ``wb`` for uploads to lakeFS.
        block_size: int | None
            The file block size to read at a time. If not set, falls back to fsspec's default blocksize of 5 MB.
        autocommit: bool
            Whether to write the file buffer automatically on file closing in write mode.
        cache_options: dict[str, str] | None
            Additional caching options to pass to the ``AbstractBufferedFile`` superclass.
        **kwargs: Any
            Additional keyword arguments to pass to ``LakeFSClient.objects_api.get_object()`` on download (``mode = 'rb'``),
            or ``LakeFSClient.objects_api.put_object()`` on upload (``mode='wb'``).

        Returns
        -------
        LakeFSFile
            A local file-like object ready to hold data to be received from / sent to a lakeFS server.

        Raises
        ------
        NotImplementedError
            If ``mode`` is not supported.
        """
        if mode not in {"rb", "wb"}:
            raise NotImplementedError(f"unsupported mode {mode!r}")

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
        lpath: str | os.PathLike[str],
        rpath: str | os.PathLike[str],
        callback: fsspec.callbacks.Callback = _DEFAULT_CALLBACK,
        presign: bool = False,
        storage_options: dict[str, Any] | None = None,
    ) -> None:
        """
        Upload a file to lakeFS by directly putting it into its underlying block storage, thereby reducing the request load
        on the lakeFS server.

        Requires the corresponding fsspec implementation for the block storage type used by your lakeFS server deployment.

        Supported block storage types are S3 (needs ``s3fs``), GCS (needs ``gcsfs``), and Azure Blob Storage (needs ``adlfs``).

        Note that depending on the block store type, additional configuration like credentials may need to be configured when ``presign=False``.

        Parameters
        ----------
        lpath: str | os.PathLike[str]
            The local path to upload to the lakeFS block storage.
        rpath: str | os.PathLike[str]
            The remote target path to upload the local file to. Must be a fully qualified lakeFS URI.
        callback: fsspec.callbacks.Callback
            An fsspec callback to use during the operation. Can be used to report download progress.
        presign: bool
            Whether to use pre-signed URLs to upload the object via HTTP(S) using ``urllib.request``.
        storage_options: dict[str, Any] | None
            Additional file system configuration options to pass to the block storage file system.

        Raises
        ------
        ValueError
            If the blockstore type returned by the lakeFS API is not supported by fsspec.
        """
        rpath = stringify_path(rpath)
        lpath = stringify_path(lpath)
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
                        logger.debug(f"Begin upload of {lpath}")
                        with urllib.request.urlopen(request):  # nosec [B310:blacklist] # We catch faulty protocols above.
                            logger.debug(f"Successfully uploaded {lpath}")
                except urllib.error.HTTPError as e:
                    raise translate_lakefs_error(e, rpath=rpath)
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
        lpath: str | os.PathLike[str],
        rpath: str | os.PathLike[str],
        callback: fsspec.callbacks.Callback = _DEFAULT_CALLBACK,
        precheck: bool = True,
        use_blockstore: bool = False,
        presign: bool = False,
        storage_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Upload a local file to a remote location on a lakeFS server.

        Note that depending on the block store type, additional configuration like credentials may need to be configured when ``use_blockstore=True`` and ``presign=False``.

        Parameters
        ----------
        lpath: str | os.PathLike[str]
            The local path on disk to upload to the lakeFS server.
        rpath: str | os.PathLike[str]
            The remote target path to upload the local file to. Must be a fully qualified lakeFS URI.
        callback: fsspec.callbacks.Callback
            An fsspec callback to use during the operation. Can be used to report download progress.
        precheck: bool
            Check if ``lpath`` already exists and compare its checksum with that of ``rpath``, skipping the download if they match.
        use_blockstore: bool
            Optionally upload the file directly to the underlying block storage, thereby bypassing the lakeFS server and saving a
            file transfer. Preferable for uploads of large files.
        presign: bool
            Whether to use pre-signed URLs to upload the object if ``use_blockstore=True``.
        storage_options: dict[str, Any] | None
            Additional file system configuration options to pass to the block storage file system if ``use_blockstore=True``.
        **kwargs: Any
            Additional keyword arguments to pass to ``AbstractFileSystem.open()``.
        """
        lpath = stringify_path(lpath)
        rpath = stringify_path(rpath)
        if precheck and Path(lpath).is_file():
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
            with self.wrapped_api_call(rpath=rpath):
                super().put_file(lpath, rpath, callback=callback, **kwargs)

    def rm_file(self, path: str | os.PathLike[str]) -> None:
        """
        Stage a remote file for removal on a lakeFS server.

        The file will not actually be removed from the requested branch until a commit is created.

        Parameters
        ----------
        path: str | os.PathLike[str]
            The remote file to delete. Must be a fully qualified lakeFS URI.
        """
        path = stringify_path(path)
        repository, branch, resource = parse(path)

        with self.wrapped_api_call(rpath=path):
            self.client.objects_api.delete_object(
                repository=repository, branch=branch, path=resource
            )
            # Directory listing cache for the containing folder must be invalidated
            self.dircache.pop(self._parent(path), None)

    def touch(self, path: str | os.PathLike[str], truncate: bool = True, **kwargs: Any) -> None:
        """
        Create an empty file or update an existing file on a lakeFS server.

        Parameters
        ----------
        path: str | os.PathLike[str]
            The file path to create or update. Must be a fully qualified lakeFS URI.
        truncate: bool
            Whether to set the file size to 0 (zero) bytes, even if the path already exists.
        **kwargs: Any
            Additional keyword arguments to pass to ``LakeFSFile.open()``.

        Raises
        ------
        NotImplementedError
            If the targeted lakeFS server version does not support `touch()` operations.
        """

        # empty buffer upload errors were fixed in https://github.com/treeverse/lakeFS/issues/7130,
        # which was first released in lakeFS v1.3.1.
        if self._lakefs_server_version < (1, 3, 1):
            version_string = ".".join(str(v) for v in self._lakefs_server_version)
            raise NotImplementedError(
                "LakeFSFileSystem.touch() is not supported for your lakeFS server version. "
                f"minimum required version: '1.3.1', actual version: {version_string!r}"
            )

        super().touch(path=path, truncate=truncate, **kwargs)


class LakeFSFile(AbstractBufferedFile):
    """
    lakeFS file implementation.

    Notes
    -----
    Creates a local buffer on disk for the given remote path for up- or downloads depending on ``mode``.

    Read operations are buffered, write operations are unbuffered. This means that local files to be uploaded will be loaded entirely into memory.

    Parameters
    ----------
    fs: LakeFSFileSystem
        The lakeFS file system associated to this file.
    path: str | os.PathLike[str]
        The remote path to either up- or download depending on ``mode``. Must be a fully qualified lakeFS URI.
    mode: Literal["rb", "wb"]
        The file mode indicating its purpose. Use ``rb`` for downloads from lakeFS, ``wb`` for uploads to lakeFS.
    block_size: int | str
        The file block size to read at a time. If not set, falls back to fsspec's default blocksize of 5 MB.
    autocommit: bool
        Whether to write the file buffer automatically to lakeFS on file closing in write mode.
    cache_type: str
        Cache policy in read mode (any of ``readahead``, ``none``, ``mmap``, ``bytes``). See ``AbstractBufferedFile`` for details.
    cache_options: dict[str, Any] | None
        Additional options passed to the constructor for the cache specified by ``cache_type``.
    size: int | None
        If given and ``mode='rb'``, this will be used as the file size (in bytes) instead of determining it from the remote file.
    **kwargs: Any
        Additional keyword arguments to pass to ``LakeFSClient.objects_api.get_object()`` on download (``mode='rb'``),
        or ``LakeFSClient.objects_api.put_object()`` on upload (``mode='wb'``).
    """

    def __init__(
        self,
        fs: LakeFSFileSystem,
        path: str | os.PathLike[str],
        mode: Literal["rb", "wb"] = "rb",
        block_size: int | str = "default",
        autocommit: bool = True,
        cache_type: str = "readahead",
        cache_options: dict[str, Any] | None = None,
        size: int | None = None,
        **kwargs: Any,
    ):
        path = stringify_path(path)
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
            create_branch(self.fs.client, repository, branch, self.fs.source_branch)

    def __del__(self):
        """Custom deleter, only here to unset the base class behavior."""
        pass

    def _upload_chunk(self, final: bool = False) -> bool:
        """
        Commit the file on final chunk via single-shot upload, no-op otherwise.

        Parameters
        ----------
        final: bool
            Proceed with uploading the file if ``self.autocommit=True``.

        Returns
        -------
        bool
            If the file buffer needs more data to be written before initiating the upload.
        """
        if final and self.autocommit:
            self.commit()
        return not final

    def commit(self):
        """
        Upload the file to lakeFS in single-shot mode.

        Results in an unbuffered upload, and a memory allocation in the magnitude of the file size on the caller's host machine.
        """
        repository, branch, resource = parse(self.path)

        with self.fs.wrapped_api_call(rpath=self.path):
            # empty buffer is equivalent to a touch()
            self.buffer.seek(0)
            self.fs.client.objects_api.upload_object(
                repository=repository,
                branch=branch,
                path=resource,
                content=self.buffer.read(),
                **self.kwargs,
            )

        self.buffer = io.BytesIO()

    def discard(self):
        """Discard the file's current buffer."""
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

        Raises
        ------
        ValueError
            If the file is closed, or has already been forcibly flushed and ``force=True``.
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
        """
        Fetch a byte range of the ``LakeFSFile``'s target remote path.

        The byte range is right-exclusive, meaning that the amount of transferred bytes equals ``end - start``.

        Parameters
        ----------
        start: int
            Start of the byte range, inclusive.
        end: int
            End of the byte range, exclusive. Must be greater than ``start``.

        Returns
        -------
        bytes
            A byte array holding the downloaded data from lakeFS.
        """
        repository, ref, resource = parse(self.path)
        with self.fs.wrapped_api_call(rpath=self.path):
            return self.fs.client.objects_api.get_object(
                repository, ref, resource, range=f"bytes={start}-{end - 1}", **self.kwargs
            )
