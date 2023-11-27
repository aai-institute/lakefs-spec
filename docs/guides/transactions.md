# Using transactions on the lakeFS file system

In addition to file operations, you can carry out versioning operations in your Python code using file system *transactions*.

A transaction is basically a context manager that collects all file uploads, defers them, and executes the uploads on completion of the transaction.
They are an "all or nothing" proposition: If an error occurs during the transaction, none of the queued files are uploaded.

The main features of the lakeFS file system transaction are:

## Thread safety

Through its use of `collections.deque` as a store for uploads, upload queueing and file transfers are thread-safe.

## Atomicity

If an exception occurs anywhere during the transaction, all queued file uploads are discarded:

```python
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

with fs.transaction as tx:
    fs.put_file("my-file.txt", "repo/main/my-file.txt")
    tx.commit("repo", "main", message="Add my-file.txt")
    raise ValueError("oops!")
```

The above code will not produce a commit on `main`, since the `ValueError` prompts a discard of the full upload queue. 

## Versioning helpers

The lakeFS file system's transaction is the intended place for conducting versioning operations between file transfers.
The following is an example of file uploads with commit creations, with a tag being applied at the end.

```python
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

with fs.transaction as tx:
    fs.put_file("train-data.txt", "repo/main/train-data.txt")
    tx.commit("repo", "main", message="Add training data")
    fs.put_file("test-data.txt", "repo/main/test-data.txt")
    sha = tx.commit("repo", "main", message="Add test data")
    tx.tag("repo", sha, tag="My train-test split")
```

!!! Warning

    All of the operations above are deferred, and their results are not available until completion of the transaction.
    For example, the `sha` return value of `tx.commit` will be a placeholder for the actual commit SHA computed by the lakeFS
    server on commit creation.

    While you can use some values (branch/tag names) returned by transaction versioning helpers, we strongly
    advise not to reuse values computed in transactions since they might result in unexpected behavior.
