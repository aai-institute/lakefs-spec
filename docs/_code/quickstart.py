from pathlib import Path

from lakefs_spec import LakeFSFileSystem

REPO, BRANCH = "repo", "main"

# Prepare example local data
local_path = Path("demo.txt")
local_path.write_text("Hello, lakeFS!")

fs = LakeFSFileSystem()  # will auto-discover credentials from ~/.lakectl.yaml
repo_path = f"{REPO}/{BRANCH}/{local_path.name}"

with fs.transaction as tx:
    fs.put(str(local_path), repo_path)
    tx.commit(REPO, BRANCH, "Add demo data")

f = fs.open(repo_path, "rt")
print(f.readline())  # prints "Hello, lakeFS!"
