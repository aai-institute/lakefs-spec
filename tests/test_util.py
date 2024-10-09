import pytest

from lakefs_spec.util import _batched


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
