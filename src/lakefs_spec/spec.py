import hashlib
import io
import logging
import os
import re
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator, NamedTuple

from fsspec.callbacks import NoOpCallback
from fsspec.spec import AbstractBufferedFile, AbstractFileSystem
from fsspec.utils import isfilelike, stringify_path
from lakefs_client import Configuration
from lakefs_client.client import LakeFSClient
from lakefs_client.exceptions import (
    ApiException,
    ForbiddenException,
    NotFoundException,
    UnauthorizedException,
)
from lakefs_client.models import BranchCreation, ObjectStatsList

from lakefs_spec.commithook import CommitHook, Default, FSEvent, HookContext

_DEFAULT_CALLBACK = NoOpCallback()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))

EmptyYield = Generator[None, None, None]


class LakectlConfig(NamedTuple):
    host: str | None = None
    username: str | None = None
    password: str | None = None

    @classmethod
    def read(cls, path: str | Path) -> "LakectlConfig":
        try:
            import yaml
        except ModuleNotFoundError:
            logger.warning(
                f"Configuration '{path}' exists, but cannot be read "
                f"because the `pyyaml package` is not installed. "
                f"To fix, run `pip install --upgrade pyyaml`.",
            )
            return cls()

        with open(path, "r") as f:
            obj: dict[str, Any] = yaml.safe_load(f)

        # config struct schema (Golang backend code):
        # https://github.com/treeverse/lakeFS/blob/master/cmd/lakectl/cmd/root.go
        creds: dict[str, str] = obj.get("credentials", {})
        server: dict[str, str] = obj.get("server", {})
        username = creds.get("access_key_id")
        password = creds.get("secret_access_key")
        host = server.get("endpoint_url")
        return cls(host=host, username=username, password=password)


@contextmanager
def translate_exceptions(path: str) -> EmptyYield:
    """
    A context manager to translate lakeFS API exceptions / error codes
    to file exceptions. This is convenience for the user to not have to
    adjust their exception handling to any lakeFS specifics.

    Specifically meant to be applied to lakeFS API endpoints.
    """
    try:
        yield
    except NotFoundException as e:
        raise FileNotFoundError(path) from e
    except ForbiddenException as e:
        raise PermissionError(path) from e
    except UnauthorizedException as e:
        raise PermissionError(f"{path!r} (unauthorized)") from e
    except ApiException as e:
        raise IOError(f"HTTP {e.status}: {e.reason}")


def md5_checksum(lpath: str, blocksize: int = 2**22) -> str:
    with open(lpath, "rb") as f:
        file_hash = hashlib.md5(usedforsecurity=False)
        chunk = f.read(blocksize)
        while chunk:
            file_hash.update(chunk)
            chunk = f.read(blocksize)
    return file_hash.hexdigest()


def parse(path: str) -> tuple[str, str, str]:
    """
    Parses a lakeFS URI in the form ``<repo>/<ref>/<resource>``.

    Parameters
    ----------
    path: str
     String path, needs to conform to the lakeFS URI format described above.
     The ``<resource>`` part can be the empty string.

    Returns
    -------
    str
       A 3-tuple of repository name, reference, and resource name.
    """

    # First regex reflects the lakeFS repository naming rules:
    # only lowercase letters, digits and dash, no leading dash,
    # minimum 3, maximum 63 characters
    # https://docs.lakefs.io/understand/model.html#repository
    # Second regex is the branch: Only letters, digits, underscores
    # and dash, no leading dash
    path_regex = re.compile(r"([a-z0-9][a-z0-9\-]{2,62})/(\w[\w\-]*)/(.*)")
    results = path_regex.fullmatch(path)
    if results is None:
        raise ValueError(f"expected path with structure <repo>/<ref>/<resource>, got {path!r}")

    repo, ref, resource = results.groups()
    return repo, ref, resource


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
        postcommit: bool = False,
        commithook: CommitHook = Default,
        precheck_files: bool = True,
        create_branch_ok: bool = True,
        source_branch: str = "main",
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
        postcommit: bool
            Whether to create lakeFS commits on the chosen branch after mutating operations,
            e.g. uploading or removing a file.
        commithook: CommitHook
            A function taking the fsspec event name (e.g. ``put_file`` for file uploads)
             and the rpath (path relative to the repository root). Must return
             a ``CommitCreation`` object, which is used to create a lakeFS commit
             for the previous file operation. Only applies to mutating operations, and when
             ``postcommit = True``.
        precheck_files: bool
            Whether to compare MD5 checksums of local and remote objects before file
            operations, and skip these operations if checksums match.
        create_branch_ok: bool
            Whether to create branches implicitly when not-existing branches are referenced on file uploads.
        source_branch: str
            Source branch set as origin when a new branch is implicitly created.
        """
        super().__init__()

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
        self.postcommit = postcommit
        self.commithook = commithook
        self.precheck_files = precheck_files
        self.create_branch_ok = create_branch_ok
        self.source_branch = source_branch

    def _rm(self, path):
        raise NotImplementedError

    @classmethod
    def _strip_protocol(cls, path):
        """Copied verbatim from the base class, save for the slash rstrip."""
        if isinstance(path, list):
            return [cls._strip_protocol(p) for p in path]
        path = stringify_path(path)
        protos = (cls.protocol,) if isinstance(cls.protocol, str) else cls.protocol
        for protocol in protos:
            if path.startswith(protocol + "://"):
                path = path[len(protocol) + 3 :]
            elif path.startswith(protocol + "::"):
                path = path[len(protocol) + 2 :]
        # use of root_marker to make minimum required path, e.g., "/"
        return path or cls.root_marker

    @contextmanager
    def scope(
        self,
        postcommit: bool | None = None,
        precheck_files: bool | None = None,
        create_branch_ok: bool | None = None,
        source_branch: str | None = None,
    ) -> EmptyYield:
        """
        Creates a context manager scope in which the lakeFS file system behavior
        is changed from defaults.

        Either post-write-operation commits, pre-operation checksum verification,
        or both can be selectively enabled or disabled.
        """
        curr_postcommit, curr_precheck_files, curr_create_branch_ok, curr_source_branch = (
            self.postcommit,
            self.precheck_files,
            self.create_branch_ok,
            self.source_branch,
        )
        try:
            if postcommit is not None:
                self.postcommit = postcommit
            if precheck_files is not None:
                self.precheck_files = precheck_files
            if create_branch_ok is not None:
                self.create_branch_ok = create_branch_ok
            if source_branch is not None:
                self.source_branch = source_branch
            yield
        finally:
            self.postcommit = curr_postcommit
            self.precheck_files = curr_precheck_files
            self.create_branch_ok = curr_create_branch_ok
            self.source_branch = curr_source_branch

    def checksum(self, path):
        try:
            return self.info(path).get("checksum", None)
        except FileNotFoundError:
            return None

    def commit(self, fsevent: FSEvent, repository: str, branch: str, resource: str) -> None:
        diff = self.client.branches_api.diff_branch(repository=repository, branch=branch)

        if not diff.results:
            logger.warning(f"No changes to commit on branch {branch!r}, aborting commit.")
            return

        ctx = HookContext(repository, branch, resource, diff)
        commit_creation = self.commithook(fsevent, ctx)
        self.client.commits_api.commit(
            repository=repository, branch=branch, commit_creation=commit_creation
        )

    def exists(self, path, **kwargs):
        repository, ref, resource = parse(path)
        try:
            self.client.objects_api.head_object(repository, ref, resource)
            return True
        except NotFoundException:
            return False

    def get_file(
        self,
        rpath,
        lpath,
        callback=_DEFAULT_CALLBACK,
        outfile=None,
        **kwargs,
    ):
        # no call to self._strip_protocol here, since that is handled by the
        # AbstractFileSystem.get() implementation
        repository, ref, resource = parse(rpath)

        if self.precheck_files and Path(lpath).exists():
            local_checksum = md5_checksum(lpath, blocksize=self.blocksize)
            remote_checksum = self.checksum(rpath)
            if local_checksum == remote_checksum:
                logger.info(
                    f"Skipping download of resource {rpath!r} to local path {lpath!r}: "
                    f"Resource {lpath!r} exists and checksums match."
                )
                return

        if isfilelike(lpath):
            outfile = lpath
        else:
            outfile = open(lpath, "wb")

        try:
            res: io.BufferedReader = self.client.objects_api.get_object(repository, ref, resource)
            while True:
                chunk = res.read(self.blocksize)
                if not chunk:
                    break
                outfile.write(chunk)
        except NotFoundException:
            raise FileNotFoundError(
                f"resource {resource!r} does not exist on ref {ref!r} in repository {repository!r}"
            )
        except ApiException as e:
            raise FileNotFoundError(f"Error (HTTP{e.status}): {e.reason}") from e
        finally:
            if not isfilelike(lpath):
                outfile.close()

            exc_type, _, __ = sys.exc_info()
            if exc_type:
                from fsspec.implementations.local import LocalFileSystem

                LocalFileSystem().rm_file(lpath)

    def info(self, path, **kwargs):
        path = self._strip_protocol(path)
        out = self.ls(path, detail=True, **kwargs)

        resource = path.split("/", maxsplit=2)[-1]
        # input path is a file name
        if len(out) == 1:
            return out[0]
        # input path is a directory name
        elif len(out) > 1:
            return {
                "name": resource,
                "size": sum(o.get("size", 0) for o in out),
                "type": "directory",
            }
        else:
            raise FileNotFoundError(resource)

    def ls(self, path, detail=True, amount=100, **kwargs):
        path = self._strip_protocol(path)
        repository, ref, prefix = parse(path)

        has_more, after = True, ""
        # stat infos are either the path only (`detail=False`) or a dict full of metadata
        info: list[Any] = []

        while has_more:
            try:
                res: ObjectStatsList = self.client.objects_api.list_objects(
                    repository,
                    ref,
                    user_metadata=detail,
                    after=after,
                    prefix=prefix,
                    amount=amount,
                )
            except NotFoundException:
                raise FileNotFoundError(
                    f"resource {prefix!r} does not exist on ref {ref!r} "
                    f"in repository {repository!r}"
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

        if not detail:
            return [o["name"] for o in info]
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
            block_size=block_size,
            autocommit=autocommit,
            cache_options=cache_options,
            **kwargs,
        )

    def put_file(
        self,
        lpath,
        rpath,
        callback=_DEFAULT_CALLBACK,
        **kwargs,
    ):
        repository, branch, resource = parse(rpath)

        if self.precheck_files:
            # TODO (n.junge): Make this work for lpaths that are themselves lakeFS paths
            local_checksum = md5_checksum(lpath, blocksize=self.blocksize)
            remote_checksum = self.checksum(rpath)
            if local_checksum == remote_checksum:
                logger.info(
                    f"Skipping upload of resource {lpath!r} to remote path {rpath!r}: "
                    f"Resource {rpath!r} exists and checksums match."
                )
                return

        with open(lpath, "rb") as f:
            self.client.objects_api.upload_object(
                repository=repository, branch=branch, path=resource, content=f
            )

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

        if self.postcommit:
            self.commit(FSEvent.PUT, repository=repository, branch=branch, resource=resource)

    def rm_file(self, path):
        repository, branch, resource = parse(path)

        try:
            self.client.objects_api.delete_object(
                repository=repository, branch=branch, path=resource
            )
        except NotFoundException:
            raise FileNotFoundError(
                f"object {resource!r} does not exist on branch {branch!r} "
                f"in repository {repository!r}"
            )

    def rm(self, path, recursive=False, maxdepth=None):
        super().rm(path, recursive=recursive, maxdepth=maxdepth)
        if self.postcommit:
            repository, branch, resource = parse(path)
            self.commit(FSEvent.RM, repository=repository, branch=branch, resource=resource)


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
            logger.warning(
                "Calling open() in write mode results in unbuffered file uploads, "
                "because the lakeFS Python client does not support multipart uploads. "
                "Note that uploading large files unbuffered can have performance implications."
            )
            repository, branch, resource = parse(path)
            ensure_branch(self.fs.client, repository, branch, self.fs.source_branch)

    def _upload_chunk(self, final=False):
        """Single-chunk (unbuffered) upload, on final (i.e. during file.close())."""
        if final:
            repository, branch, resource = parse(self.path)

            try:
                # single-shot upload.
                # empty buffer is equivalent to a touch()
                self.buffer.seek(0)
                self.fs.client.objects.upload_object(
                    repository=repository,
                    branch=branch,
                    path=resource,
                    content=self.buffer,
                )
                if self.fs.postcommit:
                    self.fs.commit(
                        FSEvent.PUT, repository=repository, branch=branch, resource=resource
                    )
            except ApiException as e:
                raise OSError(f"file upload {self.path!r} failed") from e

        return not final

    def _initiate_upload(self):
        """No-op."""
        return

    def _fetch_range(self, start: int, end: int) -> bytes:
        repository, ref, resource = parse(self.path)
        try:
            res: io.BufferedReader = self.fs.client.objects.get_object(
                repository, ref, resource, range=f"bytes={start}-{end - 1}"
            )
            return res.read()
        except ApiException as e:
            raise FileNotFoundError(f"Error (HTTP{e.status}): {e.reason}") from e
