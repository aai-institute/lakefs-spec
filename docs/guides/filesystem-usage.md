# How to use the lakeFS file system

This guide contains instructions and code snippets on how to use the lakeFS file system.

## The lakeFS URI structure

In the following subsections, we frequently make use of [lakeFS URIs](https://docs.lakefs.io/understand/model.html#lakefs-protocol-uris) in the example code.
lakeFS URIs identify resources in a lakeFS deployment through a unique path consisting of repository name, lakeFS revision/ref name, and file name relative to the repository root. Optionally, they may be prefixed with the `lakefs://` URI scheme (this is required when using [third-party libraries](integrations.md)).

As an example, a URI like `repo/main/file.txt` addresses the `file.txt` file on the `main` branch in the repository named `repo`.

In some lakeFS file system operations, directories are also allowed as resource names.
For example, the URI `repo/main/data/` (note the optional trailing slash) refers to the `data` directory on the `main` branch in the `repo` repository.

## On staged versus committed changes

When uploading, copying, or removing files or directories from a branch, those removal operations will result in staged changes in the repository until a commit is created.
lakeFS-spec does not create these commits automatically, since it separates file operations from versioning operations rigorously.
If you want to conduct versioning operations, like creating commits, between file transfers, the best way to do so is by using [filesystem transactions](transactions.md).

## How to use lakeFS file system APIs

The following section explains more in-depth how to use the `LakeFSFileSystem` APIs.
This section concerns the explicitly implemented operations. In addition, there are a number of file system APIs inherited from the [`AbstractFileSystem` interface in fsspec](https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.spec.AbstractFileSystem).

More information on file system usage can be found in the [fsspec documentation](https://filesystem-spec.readthedocs.io/en/latest/usage.html#use-a-file-system).

### Uploading and downloading files

The arguably most important feature of the file system is file transfers.

#### File uploads

To upload a file, you can use the `fs.put()` and `fs.put_file()` methods. 
While `fs.put_file()` operates on single files only, the `fs.put()` API can be used for directory uploads.

```python
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

# remote path, then local target path.
fs.put_file("file.txt", "my-repo/my-ref/file.txt")
```

If you want to upload an entire directory to lakeFS, you can use the `fs.put()` API together with the `recursive=True` switch:

```python
# structure:
#   dir/
#   ├── a.txt
#   ├── b.yaml
#   ├── c.csv
#   └── ...

fs.put("dir", "my-repo/my-ref/dir", recursive=True)
```

!!! info

    The above method of file uploading results in two transfers: Once from the client to the lakeFS server, and once from the lakeFS server to the object storage.
    This can impact performance if the uploaded files are very large. To avoid this performance issue, you can also decide to write the file directly to the underlying object storage:

    ```python
    fs = LakeFSFileSystem()
    
    fs.put_file("my-repo/my-ref/file.txt", "file.txt", use_blockstore=True)
    ```

    Direct lakeFS blockstore uploads require the installation of the corresponding fsspec file system implementation through `pip`.
    For an S3-based lakeFS deployment, install the `s3fs` package. For Google Cloud Storage (GCS), install the `gcsfs` package.
    For Azure blob storage, install the `adlfs` package.

#### File downloads

To download a file, you can use the `fs.get()` or `fs.get_file()` methods.
While `fs.get_file()` downloads single files only, the `fs.get()` API can be used for recursive directory downloads.

```python
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

# remote path, then local target path.
fs.get_file("my-repo/my-ref/file.txt", "file.txt")
```

In the case of a directory in lakeFS, use the `fs.get()` API together with the `recursive=True` switch:

```python
# structure:
#   dir/
#   ├── a.txt
#   ├── b.yaml
#   ├── c.csv
#   └── ...

# downloads the entire `dir` directory (and subdirectories) into the current directory.
fs.get("my-repo/my-ref/dir", "dir", recursive=True)
```

### Checking the existence of lakeFS objects

To check the existence of a file in a given revision of a repository, you can use the `fs.exists()` API:

```python
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

my_file_exists = fs.exists("my-repo/my-ref/my-file.txt")
```

This function returns `True` if the file exists on that revision, and `False` if it does not. Errors (e.g. permission errors) will be raised, since in that case, object existence cannot be decided.

!!! Warning
    
    `fs.exists()` only works on file objects, and will return `False` if called on directories.

### Obtaining info on stored objects

To query the metadata of a single object in a lakeFS repository, use the `fs.info()` API:

```python
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

my_file_info = fs.info("my-repo/my-ref/my-file.txt")
```

The resulting `my_file_info` object is a dictionary containing useful information such as storage location of the file, creation timestamp, and size (in bytes).

You can also call `fs.info()` on directories:

```python
dir_info = fs.info("my-repo/my-ref/dir/")
```

In this case, the resulting `dir_info` object only contains the directory name, and the cumulated size of the files it contains.

### Listing directories in lakeFS

To list the files in a directory in lakeFS, use the `fs.ls()` method:

```python
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

my_dir_listing = fs.ls("my-repo/my-ref/my-dir/")
```

This returns a list of Python dictionaries containing information on the objects contained in the requested directory.
The returned objects have the same fields set as those returned by a normal `fs.info()` call on a file object.

### Deleting objects from a lakeFS branch

To delete objects from a lakeFS branch, use the `fs.rm_file()` or `fs.rm()` APIs. As before, while the former works only for single files, the latter can be used to remove entire directories with the `recursive=True` option.

```python
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

fs.rm_file("my-repo/my-branch/my-file.txt")

# removes the entire `my-dir` directory.
fs.rm("my-repo/my-branch/my-dir/", recursive=True)
```

### Copying files in a repository

To copy files on a branch or from one branch to another, use the `fs.cp_file()` or `fs.copy()` methods:

```python
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

# copies a single file on the same branch to a new location.
fs.cp_file("my-repo/branch-a/file.txt", "my-repo/branch-a/file.txt.bak")

# copies a single file from branch A to branch B.
fs.cp_file("my-repo/branch-a/file.txt", "my-repo/branch-b/file.txt")

# copies the entire `my-dir` directory from branch A to branch B (which must exist).
fs.copy("my-repo/branch-a/my-dir/", "my-repo/branch-b/my-dir/", recursive=True)
```

!!! Info

    Files and directories can only be copied between branches in the same repository, not between different repositories.

    Trying to copy to a non-existent branch will not create the branch.
