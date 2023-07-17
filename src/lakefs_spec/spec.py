import hashlib
import io
import logging
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

    def __init__(self, client: LakeFSClient, repository: str):
        super().__init__()
        self.client = client
        self.repository = repository

    def _rm(self, path):
        raise NotImplementedError

    def checksum(self, path):
        try:
            return self.info(path).get("checksum", None)
        except (ApiException, FileNotFoundError):
            return None

    def exists(self, path, ref=None, **kwargs):
        if ref is None:
            raise ValueError(
                f"unable to test existence of file {path!r}: "
                f"no lakeFS branch was specified."
            )
        try:
            self.client.objects.head_object(self.repository, ref, path)
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
        ref=None,
        force=False,
        **kwargs,
    ):
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

        if ref is None:
            raise ValueError(
                f"unable to download remote file {rpath!r} to location {lpath!r}: "
                f"no lakeFS branch was specified."
            )

        try:
            res: io.BufferedReader = self.client.objects.get_object(
                self.repository, ref, rpath
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

    def isfile(self, path):
        """Is this entry file-like?"""
        try:
            return self.info(path)["type"] == "object"
        except (ApiException, FileNotFoundError):
            return False

    def ls(self, path, detail=True, ref=None, amount=100, **kwargs):
        if ref is None:
            raise ValueError(
                f"unable to list files for resource {path!r}: "
                f"no lakeFS branch was specified."
            )
        has_more, after = True, ""
        # stat infos are either the path only (`detail=False`) or a dict full of metadata
        info: list[Any] = []

        while has_more:
            res: ObjectStatsList = self.client.objects.list_objects(
                self.repository,
                ref,
                user_metadata=detail,
                after=after,
                prefix=self._strip_protocol(path),
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
        branch=None,
        force=False,
        **kwargs,
    ):
        if not force:
            local_checksum = md5_checksum(lpath, blocksize=self.blocksize)
            remote_checksum = self.checksum(rpath)
            if remote_checksum is not None and local_checksum == remote_checksum:
                logger.info(
                    f"Skipping upload of resource {lpath!r} to remote path {rpath!r}: "
                    f"Resource {rpath!r} exists and checksums match."
                )
                return

        if branch is None:
            raise ValueError(
                f"unable to upload local file {lpath!r} to remote path {rpath!r}: "
                f"no lakeFS branch was specified."
            )

        with open(lpath, "rb") as f:
            self.client.objects.upload_object(
                repository=self.repository, branch=branch, path=rpath, content=f
            )

    def rm_file(self, path, branch=None):
        if branch is None:
            raise ValueError(
                f"unable to delete object {path!r}: " f"no lakeFS branch was specified."
            )
        if not self.exists(path, ref=branch):
            raise FileNotFoundError(
                f"object {path!r} does not exist on branch {branch!r}"
            )

        self.client.objects.delete_object(
            repository=self.repository, branch=branch, path=path
        )
