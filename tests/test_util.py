import re

import pytest

from lakefs_spec.util import _batched, _uri_parts


def test_batched_empty_iterable():
    result = list(_batched([], 3))
    assert result == []


def test_batched_single_batch():
    result = list(_batched([1, 2, 3], 3))
    assert result == [(1, 2, 3)]


def test_batched_multiple_batches():
    result = list(_batched([1, 2, 3, 4, 5, 6, 7], 3))
    assert result == [(1, 2, 3), (4, 5, 6), (7,)]


def test_batched_batch_size_greater_than_iterable():
    result = list(_batched([1, 2], 5))
    assert result == [(1, 2)]


def test_batched_invalid_batch_size():
    with pytest.raises(ValueError, match="n must be at least one"):
        list(_batched([1, 2, 3], 0))


class TestLakeFSUriPartRegexes:
    @pytest.mark.parametrize(
        "repo_name, valid",
        [
            ("my-repo", True),
            ("@@repo", False),
            ("", False),
            ("a", False),
            ("a" * 63, True),
            ("a" * 64, False),
        ],
    )
    def test_repository(self, repo_name: str, valid: bool) -> None:
        result = re.match(_uri_parts["repository"], repo_name + "/")
        if valid:
            assert result is not None
        else:
            assert result is None

    @pytest.mark.parametrize(
        "refexp, valid",
        [
            ("", False),
            ("main", True),
            ("main@", True),
            ("main~", True),
            ("main^", True),
            ("main^2", True),
            ("main^^^", True),
            ("main^1^1", True),
            ("main^1~1", True),
        ],
    )
    def test_ref_expression(self, refexp: str, valid: bool) -> None:
        result = re.match(_uri_parts["ref expression"], refexp + "/")
        if valid:
            assert result is not None
        else:
            assert result is None
