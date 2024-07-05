from typing import Any

from fsspec.asyn import AsyncFileSystem

from lakefs_spec import LakeFSFileSystem, LakeFSTransaction
from lakefs_spec.util import async_wrapper


class AsyncLakeFSFileSystem(AsyncFileSystem):
    """Asynchronous wrapper around a LakeFSFileSystem"""

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

        self._sync_fs = LakeFSFileSystem(
            host,
            username,
            password,
            api_key,
            api_key_prefix,
            access_token,
            verify_ssl,
            ssl_ca_cert,
            proxy,
            create_branch_ok,
            source_branch,
            **storage_options,
        )

    async def _rm_file(self, path, **kwargs):
        return async_wrapper(self._sync_fs.rm_file)(path)

    async def _cp_file(self, path1, path2, **kwargs):
        return async_wrapper(self._sync_fs.cp_file)(path1, path2, **kwargs)

    async def _pipe_file(self, path, value, **kwargs):
        return async_wrapper(self._sync_fs.pipe_file)(path, value, **kwargs)

    async def _cat_file(self, path, start=None, end=None, **kwargs):
        return async_wrapper(self._sync_fs.cat_file)(path, start, end, **kwargs)

    async def _put_file(self, lpath, rpath, **kwargs):
        return async_wrapper(self._sync_fs.put_file)(lpath, rpath, **kwargs)

    async def _get_file(self, rpath, lpath, **kwargs):
        return async_wrapper(self._sync_fs.get_file)(rpath, lpath, **kwargs)

    async def _info(self, path, **kwargs):
        return async_wrapper(self._sync_fs.info)(path, **kwargs)

    async def _ls(self, path, detail=True, **kwargs):
        return async_wrapper(self._sync_fs.ls)(path, detail, **kwargs)
