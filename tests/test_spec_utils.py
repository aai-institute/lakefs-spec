import pytest

from lakefs_spec.spec import parse


def test_path_parsing():
    # case 1: well-formed path input
    path = "my-repo/my-ref/resource.txt"
    repo, ref, resource = parse(path)
    assert repo == "my-repo"
    assert ref == "my-ref"
    assert resource == "resource.txt"

    # case 2: nested resource
    path = "my-repo/my-ref/my/nested/resource.txt"
    repo, ref, resource = parse(path)
    assert repo == "my-repo"
    assert ref == "my-ref"
    assert resource == "my/nested/resource.txt"

    # case 3: top-level resource
    path = "my-repo/my-ref/"
    repo, ref, resource = parse(path)
    assert repo == "my-repo"
    assert ref == "my-ref"
    assert resource == ""

    # case 4: single-character branch
    path = "my-repo/a/resource.txt"
    repo, ref, resource = parse(path)
    assert repo == "my-repo"
    assert ref == "a"
    assert resource == "resource.txt"

    # ----------------- Failure cases -------------------
    # repo name illegally begins with hyphen
    path = "-repo/my-ref/resource.txt"
    with pytest.raises(ValueError, match="expected path .*"):
        parse(path)

    # repo name contains an illegal uppercase letter
    path = "Repo/my-ref/resource.txt"
    with pytest.raises(ValueError, match="expected path .*"):
        parse(path)

    # missing repo name
    path = "my-ref/resource.txt"
    with pytest.raises(ValueError, match="expected path .*"):
        parse(path)

    # illegal branch name
    path = "my-repo/my-ref$$$/resource.txt"
    with pytest.raises(ValueError, match="expected path .*"):
        parse(path)
