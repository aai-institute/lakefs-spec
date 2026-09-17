"""
File-like objects returned by ``LakeFSFileSystem.open()``.

These are thin subclasses of the lakeFS SDK's ``ObjectReader`` and ``ObjectWriter``,
which translate lakeFS API errors into Python builtin exceptions.
"""

from collections.abc import Generator, Iterator
from contextlib import contextmanager
from typing import AnyStr, Generic, cast

from lakefs.exceptions import ServerException, api_exception_handler
from lakefs.object import LakeFSIOBase, ObjectReader, ObjectWriter
from typing_extensions import Self

from lakefs_spec.errors import translate_lakefs_error


class _ErrorTranslationMixin(LakeFSIOBase):
    """Translates lakeFS API errors into builtin Python exceptions."""

    @property
    def _rpath(self) -> str:
        return f"{self._obj.repo}/{self._obj.ref}/{self._obj.path}"

    @contextmanager
    def _translate_errors(self) -> Generator[None, None, None]:
        try:
            # Some SDK code paths (e.g. presigned uploads) raise raw ``lakefs_sdk.ApiException``s,
            # which the handler converts into ``lakefs.exceptions.ServerException``s first.
            with api_exception_handler():
                yield
        except ServerException as e:
            raise translate_lakefs_error(e, rpath=self._rpath) from e

    def __enter__(self) -> Self:
        return self


class LakeFSObjectReader(_ErrorTranslationMixin, ObjectReader, Generic[AnyStr]):
    """
    A file-like object for reading from lakeFS, raising Python builtin exceptions
    on lakeFS API errors.

    Returned by ``LakeFSFileSystem.open()`` in read mode.
    The type parameter is ``bytes`` for binary modes and ``str`` for text modes.
    """

    def read(self, n: int | None = None) -> AnyStr:
        with self._translate_errors():
            return cast(AnyStr, super().read(n))

    def readline(self, limit: int = -1) -> AnyStr:
        with self._translate_errors():
            return cast(AnyStr, super().readline(limit))

    def __iter__(self) -> Iterator[AnyStr]:
        return self

    def __next__(self) -> AnyStr:
        return cast(AnyStr, super().__next__())

    def seek(self, offset: int, whence: int = 0) -> int:
        # Seeking from the end requires a stat call to the lakeFS server.
        with self._translate_errors():
            return super().seek(offset, whence)


class LakeFSObjectWriter(_ErrorTranslationMixin, ObjectWriter, Generic[AnyStr]):
    """
    A file-like object for writing to lakeFS, raising Python builtin exceptions on lakeFS API errors.

    Returned by ``LakeFSFileSystem.open()`` in write mode. The type parameter is ``bytes`` for binary modes
    and ``str`` for text modes, mirroring ``typing.IO``.
    Data is buffered locally and only uploaded to the lakeFS server on ``close()``.
    """

    # The lakeFS SDK declares `write` with a method-scoped `AnyStr`,
    # i.e. every writer accepts both str and bytes.
    # Narrowing to the class type parameter is intentional:
    # writing str to a binary writer fails at upload time.
    def write(self, s: AnyStr) -> int:  # ty: ignore[invalid-method-override]
        return super().write(s)

    def close(self) -> None:
        with self._translate_errors():
            super().close()
