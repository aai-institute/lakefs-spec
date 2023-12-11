from pathlib import Path

from lakefs_spec.spec import LakeFSFileSystem


def test_readline(
    fs: LakeFSFileSystem,
    repository: str,
    temp_branch: str,
) -> None:
    """Checks that `fs.open()` behaves like builtin `open` for `readline(s)` APIs."""
    lpath = Path("random_file.txt")
    try:
        lpath.write_text("Hello\nmy name is\nxyz")
        with open(lpath, "rb") as f:
            native_open_line = f.readline()
            f.seek(0)
            native_open_lines = f.readlines()

        rpath = f"{repository}/{temp_branch}/{Path(lpath).name}"
        fs.put_file(lpath, rpath)

        with fs.open(rpath, "rb") as rf:
            # mode == "rb" means everything is bytes.
            assert rf.readline() == native_open_line
            rf.seek(0)
            assert rf.readlines() == native_open_lines
    finally:
        lpath.unlink(missing_ok=True)
