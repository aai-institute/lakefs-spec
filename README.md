# lakefs-spec: An `fsspec` implementation for lakeFS 

This repository contains a [filesystem-spec](https://github.com/fsspec/filesystem_spec) implementation for the [lakeFS](https://lakefs.io/) project.
Its main goal is to facilitate versioned data operations in lakeFS directly from Python code, for example using `pandas`. See the [examples](#usage) below for inspiration.

## Installation

To install the package via `pip`, run

```shell
python3 -m pip install git+https://github.com/appliedAI-Initiative/lakefs-spec.git@v0.1.0
```

or, for the bleeding edge version,

```shell
python3 -m pip install git+https://github.com/appliedAI-Initiative/lakefs-spec.git
```

To add the project as a dependency using `poetry`, use

```shell
poetry add git+https://github.com/appliedAI-Initiative/lakefs-spec.git
```

## Usage

As an example showcase, we use the lakeFS file system to read a Pandas `DataFrame` directly from a branch. To follow
this small tutorial, you should first complete Step 1 in the [lakeFS quickstart](https://docs.lakefs.io/quickstart/launch.html) by
launching an instance, and then creating a pre-populated repository by clicking the green button on the login page.

Then, run the following code to download the sample dataframe directly from the `main` branch:

```python
import pandas as pd
from lakefs_client import Configuration
from lakefs_client.client import LakeFSClient

# change these settings to match your instance's credentials
configuration = Configuration(host="localhost:8000", username="username", password="password")
client = LakeFSClient(configuration=configuration)

df = pd.read_parquet('lakefs://quickstart/main/lakes.parquet', storage_options={"client": client})
```

### Paths and URIs

The lakeFS filesystem expects URIs that follow the [lakeFS protocol](https://docs.lakefs.io/understand/model.html#lakefs-protocol-uris).
URIs need to have the form `lakefs://<repo>/<ref>/<resource>`, with the repository name, the ref name (either a branch name or a commit SHA, depending on the operation), and resource name.
The resource can be a single file name, or a directory name for recursive operations.

### Client-side caching

In order to reduce the number of IO operations, you can enable client-side caching of both uploaded and downloaded files.
Caching works by calculating the MD5 checksum of the local file, and comparing it to that of the lakeFS remote file.
If they match, the operations are cancelled, and no additional client-server communication (including up- and downloads) happens.

Client-side caching is enabled by default in the lakeFS file system, and can be controlled through the `precheck_files` argument in the constructor:

```python
from lakefs_spec.spec import LakeFSFileSystem

# The default setting, precheck_files=False disables client-side caching.
fs = LakeFSFileSystem(client, precheck_files=True)
```

### Automatic commit creation with a commit hook

Some operations, like `fs.put()` or `fs.rm()`, change the state of a lakeFS repository by changing files. According to
the lakeFS working model, these changes are tracked as _uncommitted changes_, similarly to the git version control system.

With `lakefs-spec`, you can optionally commit changes caused by file system operations directly after they are made,
by using a **commit hook**. A commit hook is a Python function taking the `fsspec` event name that caused the changes
(e.g. `put` or `rm`), as well as the remote resource path, and returning a `CommitCreation` object that is then used by
lakeFS to create a commit directly on the chosen branch.

An example of a commit hook:

```python
from lakefs_client.models import CommitCreation

def my_commit_hook(event: str, rpath: str) -> CommitCreation:
    if event == "rm":
        message = f"❌ Remove file {rpath}"
    else:
        message = f"✅ Add file {rpath}"
    
    return CommitCreation(message=message)
```

To enable automatic commits after stateful filesystem operations, set `postcommit = True` in the filesystem constructor. If you
would like to use your own commit hook, supply a Python callable with the aforementioned signature as the `commithook` argument:

```python
from lakefs_spec.spec import LakeFSFileSystem

# use the example commit hook from above
fs = LakeFSFileSystem(client, postcommit=True, commithook=my_commit_hook)
```

### Scoped filesystem behavior changes

To selectively enable or disable automatic commits or client-side caching, you can use a `scope` context manager:

```python
from lakefs_spec.spec import LakeFSFileSystem

fs = LakeFSFileSystem(client)

with fs.scope(precheck_files=False):
    # get a fresh version of the file by disabling caching checks
    fs.get("lakefs://my-repo/my-branch/my-file.txt", "my-file.txt")

# do something with the text file...
...

# create a commit on upload by enabling automatic commits in a scoped section
with fs.scope(postcommit=True):
    fs.put("my-file.txt", "lakefs://my-repo/my-branch/my-new-file.txt")
```

## Developing and contributing to `lakefs-spec`

We welcome contributions to the project! For information on the general development workflow, head over to the [contribution guide](CONTRIBUTING.md).
