from pathlib import Path

from lakefs_spec import LakeFSFileSystem

REPO, BRANCH = "repo", "main"

# Prepare example local data
local_path = Path("demo.txt")
local_path.write_text("Hello, lakeFS!")

# Upload the local file to the repo and commit
fs = LakeFSFileSystem()  # will auto-discover credentials from ~/.lakectl.yaml
repo_path = f"{REPO}/{BRANCH}/{local_path.name}"

with fs.transaction(REPO, BRANCH) as tx:
    fs.put(str(local_path), f"{REPO}/{tx.branch.id}/{local_path.name}")
    tx.commit(message="Add demo data")

# Read back the file contents
f = fs.open(repo_path, "rt")
print(f.readline())  # prints "Hello, lakeFS!"

# Compare the sizes of local file and repo
file_info = fs.info(repo_path)
print(
    f"{local_path.name}: local size: {file_info['size']}, remote size: {local_path.stat().st_size}"
)

# Get information about all files in the repo root
print(fs.ls(f"{REPO}/{BRANCH}/"))

# Delete uploaded file from the repository (and commit)
with fs.transaction(REPO, BRANCH) as tx:
    fs.rm(f"{REPO}/{tx.branch.id}/{local_path.name}")
    tx.commit(message="Delete demo data")

local_path.unlink()
