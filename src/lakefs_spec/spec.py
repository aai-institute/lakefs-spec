from __future__ import annotations

import hashlib
import io
import logging
import mimetypes
import operator
import os
import urllib.error
import urllib.request
import warnings
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

from fsspec import filesystem
from fsspec.callbacks import NoOpCallback
from fsspec.spec import AbstractBufferedFile, AbstractFileSystem
from fsspec.utils import isfilelike, stringify_path
from lakefs_client import Configuration
from lakefs_client.client import LakeFSClient
from lakefs_client.exceptions import ApiException, NotFoundException
from lakefs_client.model.staging_metadata import StagingMetadata
from lakefs_client.models import BranchCreation, ObjectCopyCreation, ObjectStatsList

from lakefs_spec.config import LakectlConfig
from lakefs_spec.errors import translate_lakefs_error
from lakefs_spec.hooks import FSEvent, HookContext, LakeFSHook, noop
from lakefs_spec.util import get_blockstore_type, parse

_DEFAULT_CALLBACK = NoOpCallback()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

EmptyYield = Generator[None, None, None]

_warn_on_fileupload = True


def md5_checksum(lpath: str, blocksize: int = 2**22) -> str:
    with open(lpath, "rb") as f:
        file_hash = hashlib.md5(usedforsecurity=False)
        chunk = f.read(blocksize)
        while chunk:
            file_hash.update(chunk)
            chunk = f.read(blocksize)
    return file_hash.hexdigest()


def ensure_branch(client: LakeFSClient, repository: str, branch: str, source_branch: str) -> None:
    """
    Checks if a branch exists. If not, it is created.
    This implementation depends on server-side error handling.

    Parameters
    ----------
    client: LakeFSClient
        The lakeFS client configured for (and authenticated with) the target instance.
    repository: str
        Repository name.
    branch: str
        Name of the branch.
    source_branch: str
        Name of the source branch the new branch is created from.

    Returns
    -------
    None
    """

    try:
        new_branch = BranchCreation(name=branch, source=source_branch)
        # client.branches_api.create_branch throws ApiException when branch exists
        client.branches_api.create_branch(repository=repository, branch_creation=new_branch)
        logger.info(f"Created new branch {branch!r} from branch {source_branch!r}.")
    except ApiException:
        pass


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

        self._hooks: dict[FSEvent, LakeFSHook] = {}

    def register_hook(self, fsevent: str, hook: LakeFSHook, clobber: bool = False) -> None:
        fsevent = FSEvent.canonicalize(fsevent)
        if not clobber and fsevent in self._hooks:
            raise RuntimeError(
                f"hook already registered for file system event '{str(fsevent)}'. "
                f"To force registration, rerun with `clobber=True`."
            )
        self._hooks[fsevent] = hook

    def deregister_hook(self, fsevent: str) -> None:
        self._hooks.pop(FSEvent.canonicalize(fsevent), None)

    def run_hook(self, fsevent: str, ctx: HookContext) -> None:
        hook = self._hooks.get(FSEvent.canonicalize(fsevent), noop)
        hook(self.client, ctx)

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
        disable_hooks: bool = False,
    ) -> EmptyYield:
        """
        A context manager yielding scope in which the lakeFS file system behavior
        is changed from defaults.
        """
        curr_create_branch_ok, curr_source_branch, curr_hooks = (
            self.create_branch_ok,
            self.source_branch,
            self._hooks,
        )
        try:
            if create_branch_ok is not None:
                self.create_branch_ok = create_branch_ok
            if source_branch is not None:
                self.source_branch = source_branch
            if disable_hooks:
                self._hooks = {}
            yield
        finally:
            self.create_branch_ok = curr_create_branch_ok
            self.source_branch = curr_source_branch
            self._hooks = curr_hooks

    def checksum(self, path: str) -> str | None:
        try:
            checksum = self.info(path).get("checksum", None)
        except FileNotFoundError:
            checksum = None

        self.run_hook(FSEvent.CHECKSUM, HookContext.new(path))
        return checksum

    def exists(self, path: str, **kwargs: Any) -> bool:
        repository, ref, resource = parse(path)
        with self.wrapped_api_call():
            try:
                self.client.objects_api.head_object(repository, ref, resource, **kwargs)
                exists = True
            except NotFoundException:
                exists = False
            finally:
                ctx = HookContext(repository=repository, ref=ref, resource=resource)
                self.run_hook(FSEvent.EXISTS, ctx)
                return exists

    def cp_file(self, path1, path2, **kwargs):
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

        self.run_hook(FSEvent.CP_FILE, HookContext.new(path1))

    def get(
        self,
        rpath,
        lpath,
        recursive=False,
        callback=_DEFAULT_CALLBACK,
        maxdepth=None,
        **kwargs,
    ):
        super().get(
            rpath, lpath, recursive=recursive, callback=callback, maxdepth=maxdepth, **kwargs
        )
        self.run_hook(FSEvent.GET, HookContext.new(rpath))

    def get_file(
        self,
        rpath,
        lpath,
        callback=_DEFAULT_CALLBACK,
        outfile=None,
        precheck=True,
        **kwargs,
    ):
        repository, ref, resource = parse(rpath)

        def run_get_file_hook():
            ctx = HookContext(repository=repository, ref=ref, resource=resource)
            self.run_hook(FSEvent.GET_FILE, ctx)

        if precheck and Path(lpath).exists():
            local_checksum = md5_checksum(lpath, blocksize=self.blocksize)
            remote_checksum = self.checksum(rpath)
            if local_checksum == remote_checksum:
                logger.info(
                    f"Skipping download of resource {rpath!r} to local path {lpath!r}: "
                    f"Resource {lpath!r} exists and checksums match."
                )
                run_get_file_hook()
                return

        if isfilelike(lpath):
            outfile = lpath
        else:
            outfile = open(lpath, "wb")

        try:
            res: io.BufferedReader = self.client.objects_api.get_object(
                repository, ref, resource, **kwargs
            )
            while True:
                chunk = res.read(self.blocksize)
                if not chunk:
                    break
                outfile.write(chunk)
        except ApiException as e:
            from fsspec.implementations.local import LocalFileSystem

            LocalFileSystem().rm_file(lpath)
            raise translate_lakefs_error(e)
        finally:
            if not isfilelike(lpath):
                outfile.close()
            run_get_file_hook()

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

        self.run_hook(FSEvent.INFO, HookContext.new(path))
        return statobj

    def ls(self, path, detail=True, **kwargs):
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

        ctx = HookContext(repository=repository, ref=ref, resource=prefix)
        self.run_hook(FSEvent.LS, ctx)

        return info

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        if mode not in {"wb", "rb"}:
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
        self, lpath, repository, branch, resource, presign=False, storage_options=None
    ):
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
                        with urllib.request.urlopen(
                            request
                        ):  # nosec [B310:blacklist] # We catch faulty protocols above.
                            logger.info(f"Successfully uploaded {lpath}")
                except urllib.error.HTTPError as e:
                    urllib_http_error_as_lakefs_api_exception = ApiException(
                        status=e.code, reason=e.reason
                    )
                    translate_lakefs_error(error=urllib_http_error_as_lakefs_api_exception)
        else:
            blockstore_type = get_blockstore_type(self.client)
            # lakeFS blockstore name is "azure", but Azure's fsspec registry entry is "az".
            if blockstore_type == "azure":
                blockstore_type = "az"

            if blockstore_type not in ["s3", "gs", "az"]:
                raise ValueError(
                    f"Blockstore writes are not implemented for blockstore type {blockstore_type!r}"
                )

            remote_url = staging_location.physical_address
            remote = filesystem(blockstore_type, **(storage_options or {}))
            remote.put_file(lpath, remote_url)

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
        lpath,
        rpath,
        callback=_DEFAULT_CALLBACK,
        precheck=True,
        use_blockstore=False,
        presign=False,
        storage_options=None,
        **kwargs,
    ):
        repository, branch, resource = parse(rpath)

        def run_put_file_hook():
            ctx = HookContext(repository=repository, ref=branch, resource=resource)
            self.run_hook(FSEvent.PUT_FILE, ctx)

        if precheck:
            remote_checksum = self.checksum(rpath)
            local_checksum = md5_checksum(lpath, blocksize=self.blocksize)
            if local_checksum == remote_checksum:
                logger.info(
                    f"Skipping upload of resource {lpath!r} to remote path {rpath!r}: "
                    f"Resource {rpath!r} exists and checksums match."
                )
                run_put_file_hook()
                return
        if use_blockstore:
            self.put_file_to_blockstore(
                lpath,
                repository,
                branch,
                resource,
                presign=presign,
                storage_options=storage_options,
            )
        else:
            with open(lpath, "rb") as f:
                with self.wrapped_api_call():
                    self.client.objects_api.upload_object(
                        repository=repository, branch=branch, path=resource, content=f, **kwargs
                    )

        run_put_file_hook()

    def put(
        self,
        lpath,
        rpath,
        recursive=False,
        callback=_DEFAULT_CALLBACK,
        maxdepth=None,
        **kwargs,
    ):
        repository, branch, resource = parse(rpath)
        if self.create_branch_ok:
            ensure_branch(self.client, repository, branch, self.source_branch)

        super().put(
            lpath, rpath, recursive=recursive, callback=callback, maxdepth=maxdepth, **kwargs
        )

        ctx = HookContext(repository=repository, ref=branch, resource=resource)
        self.run_hook(FSEvent.PUT, ctx)

    def rm_file(self, path):
        repository, branch, resource = parse(path)

        with self.wrapped_api_call():
            self.client.objects_api.delete_object(
                repository=repository, branch=branch, path=resource
            )

        ctx = HookContext(repository=repository, ref=branch, resource=resource)
        self.run_hook(FSEvent.RM_FILE, ctx)

    def rm(self, path, recursive=False, maxdepth=None):
        super().rm(path, recursive=recursive, maxdepth=maxdepth)

        self.run_hook(FSEvent.RM, HookContext.new(path))


class LakeFSFile(AbstractBufferedFile):
    """lakeFS file implementation. Buffered in reads, unbuffered in writes."""

    def __init__(
        self,
        fs,
        path,
        mode="rb",
        block_size="default",
        autocommit=True,
        cache_type="readahead",
        cache_options=None,
        size=None,
        **kwargs,
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
            repository, branch, resource = parse(path)
            ensure_branch(self.fs.client, repository, branch, self.fs.source_branch)

    def _upload_chunk(self, final=False):
        """Single-chunk (unbuffered) upload, on final (i.e. during file.close())."""
        if final:
            repository, branch, resource = parse(self.path)

            with self.fs.wrapped_api_call():
                # single-shot upload.
                # empty buffer is equivalent to a touch()
                self.buffer.seek(0)
                self.fs.client.objects.upload_object(
                    repository=repository,
                    branch=branch,
                    path=resource,
                    content=self.buffer,
                )

        return not final

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
            res: io.BufferedReader = self.fs.client.objects.get_object(
                repository, ref, resource, range=f"bytes={start}-{end - 1}"
            )
            return res.read()

    def close(self):
        super().close()
        if self.mode == "wb":
            self.fs.run_hook(FSEvent.FILEUPLOAD, HookContext.new(self.path))
        elif self.mode == "rb":
            self.fs.run_hook(FSEvent.FILEDOWNLOAD, HookContext.new(self.path))
