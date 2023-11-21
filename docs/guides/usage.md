# How to use the lakeFS file system

This guide contains instructions and code snippets on how to use the lakeFS file system.

## Uploading and downloading files

The arguably most important feature of the file system are file transfers.

### File uploads

To upload a file, you can use the `fs.put()` and `fs.put_file()` methods. 
While `put_file()` operates on single files only as the name suggests, the `put` API can be used for directory uploads.

```python
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

# remote path, then local target path.
fs.put_file("file.txt", "my-repo/my-ref/file.txt")
```

If you want to upload a whole directory to lakeFS, use the `fs.put` API together with the `recursive=True` switch:

```python
# structure:
#   dir/a.txt
#      /b.yaml
#      /c.csv
#      /...

from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

# creates the target directory `dir` in the current directory.
fs.put("dir", "my-repo/my-ref/dir", recursive=True)
```

!!! info

    The above method of file uploading results in two transfers: Once from the client to the lakeFS server, and once from the lakeFS server to the object storage.
    This can impact performance if the uploaded files are very large. To avoid this performance hit, you can also decide to write the file directly to the underlying object storage:

    ```python
    fs = LakeFSFileSystem()
    
    fs.put_file("my-repo/my-ref/file.txt", "file.txt", use_blockstore=True)
    ```

    Direct lakeFS blockstore uploads require the installation of the corresponding `fsspec` file system implementation through `pip`.
    For an S3-based lakeFS deployment, install the `s3fs` package. For Google Cloud Storage (GCS), install the `gcsfs` package.
    For Azure blob storage, install the `adlfs` package.

### File downloads

To download a file, you can use the `fs.get()` or `fs.get_file` methods:

```python
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

# remote path, then local target path.
fs.get_file("my-repo/my-ref/file.txt", "file.txt")
```

In the case of a directory in lakeFS, use the `fs.get()` API together with the `recursive=True` switch:

```python
# structure:
#   dir/a.txt
#      /b.yaml
#      /c.csv
#      /...

from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

# creates the target directory `dir` in the current directory.
fs.get("my-repo/my-ref/dir", "dir", recursive=True)
```

## Obtaining info on stored objects

To query (also called `stat`) a single object in a lakeFS repository, use the `fs.info()` API:

```python
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

my_file_info = fs.info("my-repo/my-ref/my-file.txt")
```

The resulting `my_file_info` object is a dictionary containing useful information such as storage location of the file, creation timestamp, and size (in bytes).

You can also call `fs.info` on directories:

```python
dir_info = fs.info("my-repo/my-ref/dir/")
```

In this case, the resulting `dir_info` object only contains the directory name, and the cumulated size of the files it contains.

## Listing directories in lakeFS

To list the files in a directory in lakeFS, use the `fs.ls()` method:

```python
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

my_dir_listing = fs.ls("my-repo/my-ref/my-dir/")
```

This returns a list of Python dictionaries containing information on the objects contained in the requested directory.
The returned objects have the same fields set as those returned by a normal `fs.info()` call on a file object.

## Deleting objects from a lakeFS branch

To delete objects from a lakeFS branch, use the `fs.rm_file()` or `fs.rm()` APIs. As before, while the former works only for single files, the latter can be used to remove whole directories with the `recursive=True` option.

```python
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

fs.rm_file("my-repo/my-branch/my-file.txt")

# removes the whole `my-dir` directory.
fs.rm("my-repo/my-branch/my-dir/", recursive=True)
```

When removing files or directories from a branch, those removal operations are staged changes until another commit is created. 

## Copying files between branches

To copy files from one branch to a different branch, use the `fs.cp_file` or `fs.copy()` methods:

```python
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

# copies a single file from branch A to branch B.
fs.cp_file("my-repo/branch-a/file.txt", "my-repo/branch-b/file.txt")

# copies the whole `my-dir` directory from branch A to B.
fs.copy("my-repo/branch-a/my-dir/", "my-repo/branch-b/my-dir/", recursive=True)
```

!!! Info

    Files and directories can only be copied between branches in the same repository, not between different repositories.
