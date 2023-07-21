import hashlib
import io
import logging
import re
import sys
from typing import Any

from fsspec.callbacks import NoOpCallback
from fsspec.spec import AbstractFileSystem
from fsspec.utils import isfilelike
from lakefs_client import ApiException
from lakefs_client.models import ObjectStatsList

from lakefs_spec.client import LakeFSClient

_DEFAULT_CALLBACK = NoOpCallback()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))


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
    Parses a lakeFS URI in the form <repo>/<ref>/<path>.

    Parameters
    ----------
    path: String path, needs to conform to the lakeFS URI format described above.

    Returns
    -------
    A tuple of repository name, reference, and resource name.

    """
    # First regex reflects the lakeFS repository naming rules:
    # only lowercase letters, digits and dash, no leading dash,
    # minimum 3, maximum 63 characters
    # https://docs.lakefs.io/understand/model.html#repository
    # Second regex is the branch: Only letters, digits, underscores
    # and dash, no leading dash
    path_regex = re.compile(r"([a-z0-9][a-z0-9\-]{2,62})/(\w[\w\-]+)/(.*)")
    results = path_regex.fullmatch(path)
    if results is None:
        raise ValueError(
            f"expected path with structure <repo>/<ref>/<resource>, got {path!r}"
        )

    return results.group(1), results.group(2), results.group(3)


class LakeFSFileSystem(AbstractFileSystem):
    """
    lakeFS file system spec implementation.

    Objects are put into a remote repository via the lakeFS API directly,
    instead of going the indirection through boto3. This allows us to direct
    all S3 resources to Flyte's storage.

    The repository is assumed immutable in an instance of the filesystem,
    the branch is not necessarily constant, though.
    """

    protocol = "lakefs"

    def __init__(self, client: LakeFSClient):
        super().__init__()
        self.client = client

    def _rm(self, path):
        raise NotImplementedError

    def checksum(self, path):
        try:
            return self.info(path).get("checksum", None)
        except (ApiException, FileNotFoundError):
            return None

    def exists(self, path, **kwargs):
        repository, ref, resource = parse(path)
        try:
            self.client.objects.head_object(repository, ref, resource)
            return True
        except ApiException as e:
            if e.status == 404:
                return False
            raise FileNotFoundError(f"Error (HTTP{e.status}): {e.reason}") from e

    def get_file(
        self,
        rpath,
        lpath,
        callback=_DEFAULT_CALLBACK,
        outfile=None,
        force=False,
        **kwargs,
    ):
        # no call to self._strip_protocol here, since that is handled by the
        # AbstractFileSystem.get() implementation
        repository, ref, resource = parse(rpath)

        if not force and super().exists(lpath):
            local_checksum = md5_checksum(lpath, blocksize=self.blocksize)
            remote_checksum = self.checksum(rpath)
            if remote_checksum is not None and local_checksum == remote_checksum:
                logger.info(
                    f"Skipping download of resource {rpath!r} to local path {lpath!r}: "
                    f"Resource {lpath!r} exists and checksums match."
                )
                return

        if isfilelike(lpath):
            outfile = lpath
        else:
            outfile = open(lpath, "wb")  # pylint: disable=consider-using-with

        try:
            res: io.BufferedReader = self.client.objects.get_object(
                repository, ref, resource
            )
            while True:
                chunk = res.read(self.blocksize)
                if not chunk:
                    break
                outfile.write(chunk)
        except ApiException as e:
            logger.error(e)
            raise FileNotFoundError(f"Error (HTTP{e.status}): {e.reason}") from e
        finally:
            if not isfilelike(lpath):
                outfile.close()

    def info(self, path, **kwargs):
        out = self.ls(path, detail=True, **kwargs)
        # TODO: Avoid double path parsing by implementing an ls overload
        #  that takes the parsed inputs instead of raw paths
        *_, resource = parse(path)
        resource = resource.rstrip("/")

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

    def isfile(self, path):
        """Is this entry file-like?"""
        try:
            return self.info(path)["type"] == "object"
        except (ApiException, FileNotFoundError):
            return False

    def ls(self, path, detail=True, amount=100, **kwargs):
        repository, ref, prefix = parse(path)

        has_more, after = True, ""
        # stat infos are either the path only (`detail=False`) or a dict full of metadata
        info: list[Any] = []

        while has_more:
            res: ObjectStatsList = self.client.objects.list_objects(
                repository,
                ref,
                user_metadata=detail,
                after=after,
                prefix=prefix,
                amount=amount,
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
                        "type": obj.path_type,
                    }
                )

        if not detail:
            return [o["name"] for o in info]
        return info

    def put_file(
        self,
        lpath,
        rpath,
        callback=_DEFAULT_CALLBACK,
        force=False,
        **kwargs,
    ):
        repository, branch, resource = parse(rpath)

        if not force:
            # TODO (n.junge): Make this work for lpaths that are themselves lakeFS paths
            local_checksum = md5_checksum(lpath, blocksize=self.blocksize)
            remote_checksum = self.checksum(rpath)
            if remote_checksum is not None and local_checksum == remote_checksum:
                logger.info(
                    f"Skipping upload of resource {lpath!r} to remote path {rpath!r}: "
                    f"Resource {rpath!r} exists and checksums match."
                )
                return

        with open(lpath, "rb") as f:
            self.client.objects.upload_object(
                repository=repository, branch=branch, path=resource, content=f
            )

    def rm_file(self, path):
        repository, branch, resource = parse(path)

        if not self.exists(path):
            raise FileNotFoundError(
                f"object {resource!r} does not exist on branch {branch!r}"
            )

        self.client.objects.delete_object(
            repository=repository, branch=branch, path=resource
        )
