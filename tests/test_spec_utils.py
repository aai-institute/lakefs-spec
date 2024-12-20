from contextlib import AbstractContextManager

import pytest

from lakefs_spec.spec import parse


@pytest.mark.parametrize(
    "path,repo,ref,resource",
    [
        # case 1: well-formed path input
        ("my-repo/my-ref/resource.txt", "my-repo", "my-ref", "resource.txt"),
        # case 2: nested resource
        ("my-repo/my-ref/my/nested/resource.txt", "my-repo", "my-ref", "my/nested/resource.txt"),
        # case 3: top-level resource
        ("my-repo/my-ref/", "my-repo", "my-ref", ""),
        # case 4: single-character branch
        ("my-repo/a/resource.txt", "my-repo", "a", "resource.txt"),
        # case 5: well-formed path with leading lakefs:// scheme
        ("lakefs://my-repo/my-ref/resource.txt", "my-repo", "my-ref", "resource.txt"),
        # case 6: ref is a tag with a dot, like e.g. in semver.
        ("lakefs://my-repo/v2.0/resource.txt", "my-repo", "v2.0", "resource.txt"),
        # case 7: ref is a relative ancestor to an existing branch, like e.g. HEAD~ in git.
        ("lakefs://my-repo/main~/resource.txt", "my-repo", "main~", "resource.txt"),
        # case 8: same as case 7, but with first-parent ancestors only.
        ("lakefs://my-repo/main^^/resource.txt", "my-repo", "main^^", "resource.txt"),
    ],
)
def test_path_parsing_success(path: str, repo: str, ref: str, resource: str) -> None:
    act_repo, act_ref, act_resource = parse(path)
    assert act_repo == repo
    assert act_ref == ref
    assert act_resource == resource


@pytest.mark.parametrize(
    "path,expected_exception",
    [
        # repo name illegally begins with hyphen
        ("-repo/my-ref/resource.txt", pytest.raises(ValueError, match="invalid repository")),
        # repo name contains an illegal uppercase letter
        ("Repo/my-ref/resource.txt", pytest.raises(ValueError, match="invalid repository")),
        # missing repo name
        ("my-ref/resource.txt", pytest.raises(ValueError, match="invalid ref expression")),
        # illegal branch name
        ("repo/my-ref$$$/resource.txt", pytest.raises(ValueError, match="invalid ref expression")),
    ],
)
def test_path_parsing_failure(path: str, expected_exception: AbstractContextManager) -> None:
    with expected_exception:
        parse(path)
