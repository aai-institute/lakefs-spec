"""
Core interface definitions for file system interaction with lakeFS from Python.

In particular, the core ``LakeFSFileSystem`` and ``LakeFSFile`` classes.
"""

import errno
import logging
import operator
import os
from collections.abc import Generator
from contextlib import contextmanager
from functools import cached_property
from pathlib import Path
from typing import Any, Literal, cast, overload

import fsspec.callbacks
import lakefs
from fsspec.callbacks import _DEFAULT_CALLBACK
from fsspec.spec import AbstractFileSystem
from fsspec.utils import stringify_path
from lakefs.client import Client
from lakefs.exceptions import NotFoundException, ServerException
from lakefs.models import CommonPrefix, ObjectInfo
from lakefs.object import LakeFSIOBase, ObjectReader, ObjectWriter

from lakefs_spec.errors import translate_lakefs_error
from lakefs_spec.transaction import LakeFSTransaction
from lakefs_spec.types import ObjectInfoData
from lakefs_spec.util import batched, md5_checksum, parse

logger = logging.getLogger("lakefs-spec")

MAX_DELETE_OBJS = 1000


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
    create_branch_ok: bool
        Whether to create branches implicitly when not-existing branches are referenced on file uploads.
    source_branch: str
        Source branch set as origin when a new branch is implicitly created.
    **storage_options: Any
        Configuration options to pass to the file system's directory cache.
    """

    protocol = "lakefs"
    transaction_type = LakeFSTransaction

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
        create_branch_ok: bool = True,
        source_branch: str = "main",
        **storage_options: Any,
    ):
        super().__init__(**storage_options)

        # lakeFS client arguments
        cargs = [host, username, password, api_key, api_key_prefix, access_token, ssl_ca_cert]

        if all(arg is None for arg in cargs):
            # empty kwargs means envvar and configfile autodiscovery
            self.client = Client()
        else:
            self.client = Client(
                host=host,
                username=username,
                password=password,
                api_key=api_key,
                api_key_prefix=api_key_prefix,
                access_token=access_token,
                ssl_ca_cert=ssl_ca_cert,
            )

        # proxy address, not part of the constructor
        self.client.config.proxy = proxy
        # whether to verify SSL certs, not part of the constructor
        self.client.config.verify_ssl = verify_ssl

        self.create_branch_ok = create_branch_ok
        self.source_branch = source_branch

    @cached_property
    def _lakefs_server_version(self):
        with self.wrapped_api_call():
            return tuple(int(t) for t in self.client.version.split("."))

    @classmethod
    @overload
    def _strip_protocol(cls, path: str | os.PathLike[str] | Path) -> str: ...

    @classmethod
    @overload
    def _strip_protocol(cls, path: list[str | os.PathLike[str] | Path]) -> list[str]: ...

    @classmethod
    def _strip_protocol(cls, path):
        """Copied verbatim from the base class, save for the slash rstrip."""
        if isinstance(path, list):
            return [cls._strip_protocol(p) for p in path]
        spath = super()._strip_protocol(path)
        if stringify_path(path).endswith("/"):
            return spath + "/"
        return spath

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
        except ServerException as e:
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
            info = self.info(path)
            if info["type"] == "file":
                return info["checksum"]
            else:
                # directories do not have a checksum
                return None
        except FileNotFoundError:
            return None

    def exists(self, path: str | os.PathLike[str], **kwargs: Any) -> bool:
        """
        Check existence of a remote path in a lakeFS repository.

        Input paths can either be files or directories.

        If the path refers to the root of the repository, this method will return
        ``True`` if the reference or branch exists.

        Parameters
        ----------
        path: str | os.PathLike[str]
            The remote path whose existence to check. Must be a fully qualified lakeFS URI.
        **kwargs: Any
            Additional keyword arguments for fsspec compatibility, unused.

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
            reference = lakefs.Reference(repository, ref, client=self.client)

            # Repo root (i.e., empty resource) boils down to checking if the ref exists
            if resource == "":
                return reference.get_commit() is not None

            if reference.object(resource).exists():
                return True
            # if it isn't an object, it might be a common prefix (i.e. "directory").
            children = reference.objects(
                max_amount=1, prefix=resource.rstrip("/") + "/", delimiter="/"
            )
            return len(list(children)) > 0
        except NotFoundException:
            return False
        except ServerException as e:
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
            Additional keyword arguments for fsspec compatibility, unused.

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
            reference = lakefs.Reference(orig_repo, orig_ref, client=self.client)
            reference.object(orig_path).copy(dest_ref, dest_path)

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

        if precheck and Path(lpath).is_file():
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

    def info(self, path: str | os.PathLike[str], **kwargs: Any) -> ObjectInfoData:
        """
        Query a remote lakeFS object's metadata.

        Parameters
        ----------
        path: str | os.PathLike[str]
            The object for which to obtain metadata. Must be a fully qualified lakeFS URI, can either point to a file or a directory.
        **kwargs: Any
            Additional keyword arguments to pass to ``LakeFSFileSystem.ls()`` if ``path`` points to a directory.

        Returns
        -------
        ObjectInfoData
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
                reference = lakefs.Reference(repository, ref, client=self.client)
                res = reference.object(resource).stat()
                return {
                    "type": "file",
                    "checksum": res.checksum,
                    "content-type": res.content_type,
                    "mtime": res.mtime,
                    "name": f"{repository}/{ref}/{res.path}",
                    "size": res.size_bytes,
                    "metadata": res.metadata,
                }
            except NotFoundException:
                # fall through, retry with `ls` if it's a directory.
                pass
            except ServerException as e:
                raise translate_lakefs_error(e, rpath=path)

        out = self.ls(path, detail=True, recursive=False, **kwargs)
        if not out:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)

        return {
            "type": "directory",
            "name": path.rstrip("/"),
            "size": sum(o.get("size") or 0 for o in out),
        }

    def _update_dircache(self, info: list) -> None:
        """Update logic for dircache (optionally recursive) based on lakeFS API response"""
        parents = {self._parent(i["name"].rstrip("/")) for i in info}
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
    ) -> list[dict[str, Any]]: ...

    @overload
    def ls(
        self,
        path: str | os.PathLike[str],
        detail: Literal[False],
        **kwargs: Any,
    ) -> list[str]: ...

    # Catch-all for non-literal `detail` argument
    @overload
    def ls(
        self,
        path: str | os.PathLike[str],
        detail: bool = True,
        **kwargs: Any,
    ) -> list[str] | list[dict[str, Any]]: ...

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
            Additional keyword arguments for fsspec compatibility.

            In particular:
                `refresh: bool`: whether to skip the directory listing cache,
                `recursive: bool`: whether to list subdirectory contents recursively

        Returns
        -------
        list[str] | list[dict[str, Any]]
            A list of all objects' metadata under the given remote path if ``detail=True``, or alternatively only their names if ``detail=False``.
        """
        path = self._strip_protocol(path)
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

        # stat infos are either the path only (`detail=False`) or a dict full of metadata
        info: list[ObjectInfoData] = []
        delimiter = "" if recursive else "/"
        reference = lakefs.Reference(repository, ref, client=self.client)

        with self.wrapped_api_call(rpath=path):
            for obj in reference.objects(prefix=prefix, delimiter=delimiter):
                if isinstance(obj, CommonPrefix):
                    # prefixes are added below.
                    info.append(
                        {
                            "name": f"{repository}/{ref}/{obj.path}",
                            "size": 0,
                            "type": "directory",
                        }
                    )
                elif isinstance(obj, ObjectInfo):
                    # Skip over prefix-only matches, e.g., given:
                    #
                    # foo/
                    # ├── bar/
                    # │   └── ...
                    # └── bar__baz.txt
                    #
                    # `ls("foo/bar")` should not include `bar__baz.txt`
                    if not (prefix == "" or prefix.endswith("/")) and obj.path != prefix:
                        continue

                    info.append(
                        {
                            "checksum": obj.checksum,
                            "content-type": obj.content_type,
                            "mtime": obj.mtime,
                            "name": f"{repository}/{ref}/{obj.path}",
                            "size": obj.size_bytes,
                            "type": "file",
                        }
                    )

        # Retry the API call with appended slash if the current result
        # is just a single directory entry only (not its contents).
        # This is useful to allow `ls("repo/branch/dir")` calls without a trailing slash.
        if len(info) == 1 and info[0]["type"] == "directory" and info[0]["name"] == path + "/":
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
                        "name": subdir + "/",
                        "size": 0,
                        "type": "directory",
                    }
                )

        if info:
            self._update_dircache(info[:])

        if not detail:
            return [o["name"] for o in info]
        else:
            return [cast(dict, o) for o in info]

    def open(
        self,
        path: str | os.PathLike[str],
        mode: Literal["r", "rb", "rt", "w", "wb", "wt", "x", "xb", "xt"] = "rb",
        pre_sign: bool | None = None,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
        autocommit: bool = True,
        **kwargs: Any,
    ) -> LakeFSIOBase:
        """
        Dispatch a lakeFS file-like object (local buffer on disk) for the given remote path for up- or downloads depending on ``mode``.

        Parameters
        ----------
        path: str | os.PathLike[str]
            The remote path for which to open a local ``LakeFSFile``. Must be a fully qualified lakeFS URI.
        mode: Literal["r", "rb", "rt", "w", "wb", "wt", "x", "xb", "xt"]
            The file mode indicating its purpose. Use ``r/rb`` for downloads from lakeFS, ``w/wb/x/xb`` for uploads to lakeFS.
        pre_sign: bool | None
            Whether to use a pre-signed URL for the file up-/download. If ``None``, the value from the storage configuration is used.
        content_type: str | None
            Content type to use for the file, relevant for uploads only.
        metadata: dict[str, str] | None
            Additional metadata to attach to the file, relevant for uploads only.
        autocommit: bool
            Whether to process the file immediately instead of queueing it for transaction while in a transaction context.
        **kwargs: Any
            Additional keyword arguments for fsspec compatibility, unused.

        Returns
        -------
        LakeFSIOBase
            A local file-like object ready to hold data to be received from / sent to a lakeFS server.

        Raises
        ------
        NotImplementedError
            If ``mode`` is not supported.
        """
        if mode.endswith("t"):
            # text modes {r,w,x}t are equivalent to {r,w,x} here respectively.
            mode = mode[:-1]  # type: ignore

        if mode not in {"r", "rb", "w", "wb", "x", "xb"}:
            raise NotImplementedError(f"unsupported mode {mode!r}")

        path = stringify_path(path)
        repo, ref, resource = parse(path)

        if mode.startswith("r"):
            reference = lakefs.Reference(repo, ref, client=self.client)
            obj = reference.object(resource)

            if not obj.exists():
                raise FileNotFoundError(path)
            handler = ObjectReader(obj, mode=mode, pre_sign=pre_sign, client=self.client)
        else:
            # for writing ops, ref must be a branch
            branch = lakefs.Branch(repo, ref, client=self.client)
            if self.create_branch_ok:
                branch.create(self.source_branch, exist_ok=True)

            obj = branch.object(resource)
            handler = ObjectWriter(
                obj,
                mode=mode,
                pre_sign=pre_sign,
                content_type=content_type,
                metadata=metadata,
                client=self.client,
            )

        ac = kwargs.pop("autocommit", not self._intrans)
        if not ac and "r" not in mode:
            self._transaction.files.append(handler)

        return handler

    def put_file(
        self,
        lpath: str | os.PathLike[str],
        rpath: str | os.PathLike[str],
        callback: fsspec.callbacks.Callback = _DEFAULT_CALLBACK,
        precheck: bool = True,
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
        **kwargs: Any
            Additional keyword arguments to pass to ``LakeFSFileSystem.open()``.
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

        with self.wrapped_api_call(rpath=rpath):
            super().put_file(lpath, rpath, callback=callback, **kwargs)

    def rm_file(self, path: str | os.PathLike[str]) -> None:  # pragma: no cover
        """
        Stage a remote file for removal on a lakeFS server.

        The file will not actually be removed from the requested branch until a commit is created.

        Parameters
        ----------
        path: str | os.PathLike[str]
            The remote file to delete. Must be a fully qualified lakeFS URI.
        """
        self.rm(path)

    def rm(
        self, path: str | os.PathLike[str], recursive: bool = False, maxdepth: int | None = None
    ) -> None:
        """
        Stage multiple remote files for removal on a lakeFS server.

        The files will not actually be removed from the requested branch until a commit is created.

        Parameters
        ----------
        path: str | os.PathLike[str]
            File(s) to delete.
        recursive: bool
            If file(s) include nested directories, recursively delete their contents.
        maxdepth: int | None
            Depth to pass to walk for finding files to delete, if recursive.
            If None, there will be no limit and infinite recursion may be
            possible.
        """

        path = stringify_path(path)
        repository, ref, prefix = parse(path)

        with self.wrapped_api_call(rpath=path):
            branch = lakefs.Branch(repository, ref, client=self.client)
            objgen_batched = batched(
                branch.objects(prefix=prefix, delimiter="" if recursive else "/"), n=MAX_DELETE_OBJS
            )
            if maxdepth is None:
                for objgen in objgen_batched:
                    branch.delete_objects(obj.path for obj in objgen)
            else:
                for objgen in objgen_batched:
                    # nesting level is just the amount of "/"s in the path, no leading "/".
                    branch.delete_objects(
                        obj.path for obj in objgen if obj.path.count("/") <= maxdepth
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
            Additional keyword arguments to pass to ``LakeFSFileSystem.open()``.

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

    def tail(self, path: str | os.PathLike[str], size: int = 1024) -> bytes:
        """
        Get the last ``size`` bytes from a remote file.

        Parameters
        ----------
        path: str | os.PathLike[str]
            The file path to read. Must be a fully qualified lakeFS URI.
        size: int
            The amount of bytes to get.

        Returns
        -------
        bytes
            The bytes at the end of the requested file.
        """
        f: ObjectReader
        with self.open(path, "rb") as f:
            f.seek(max(-size, -f._obj.stat().size_bytes), 2)
            return f.read()
