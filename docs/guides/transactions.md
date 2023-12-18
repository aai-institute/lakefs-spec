# Using transactions on the lakeFS file system

In addition to file operations, you can carry out versioning operations in your Python code using file system *transactions*.

A transaction is basically a context manager that collects all file uploads, defers them, and executes the uploads on completion of the transaction.
They are an "all or nothing" proposition: If an error occurs during the transaction, none of the queued files are uploaded.
For more information on fsspec transactions, see the official [documentation](https://filesystem-spec.readthedocs.io/en/latest/features.html#transactions).

The main features of the lakeFS file system transaction are:

## Atomicity

If an exception occurs anywhere during the transaction, all queued file uploads and versioning operations are discarded:

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

The full list of supported lakeFS versioning operations:

* [`commit`](../reference/lakefs_spec/transaction.md#lakefs_spec.transaction.LakeFSTransaction.commit), for creating commits on a branch, optionally with attached metadata.
* [`create_branch`](../reference/lakefs_spec/transaction.md#lakefs_spec.transaction.LakeFSTransaction.create_branch), for creating a new branch.
* [`merge`](../reference/lakefs_spec/transaction.md#lakefs_spec.transaction.LakeFSTransaction.merge), for merging a given branch into another branch.
* [`revert`](../reference/lakefs_spec/transaction.md#lakefs_spec.transaction.LakeFSTransaction.revert), for reverting a previous commit on a branch.
* [`rev_parse`](../reference/lakefs_spec/transaction.md#lakefs_spec.transaction.LakeFSTransaction.rev_parse), for parsing revisions like branch/tag names and SHA fragments into full commit SHAs.
* [`tag`](../reference/lakefs_spec/transaction.md#lakefs_spec.transaction.LakeFSTransaction.tag), for creating a tag pointing to a commit.

!!! Warning

    All of the operations above are deferred, and their results are not available until completion of the transaction.
    For example, the `sha` return value of `tx.commit` will be a placeholder for the actual commit SHA computed by the lakeFS server on commit creation.

    While you can directly use some values (branch/tag names) returned by transaction versioning helpers, care needs to be taken with computed objects like commit SHAs:

    ```python
    with fs.transaction as tx:
        fs.put_file("my-file.txt", "repo/branch/my-file.txt")
        sha = tx.commit("repo", "branch", message="Add my-file.txt")
    
    # This will not work: `sha` is of type `Placeholder[Commit]`
    fs.get_file(f"repo/{sha}/my-file.txt", "my-new-file.txt")
    ```
    
    See the following section on how to reuse commits created during transactions. 

### Reusing resources created in transactions

Some transaction versioning helpers create new objects in the lakeFS instance that are not known before said helpers are actually executed.
An example of this is a commit SHA, which is only available once created by the lakeFS server.
In the above example, a commit is created directly after a file upload, but its actual SHA identifier will not be available until the transaction is complete.
After the transaction is completed, you can reuse the computed value (a [`Placeholder`](../reference/lakefs_spec/transaction.md#lakefs_spec.transaction.Placeholder) object) in your code like you would any other lakeFS server result:

```python
with fs.transaction as tx:
    fs.put_file("my-file.txt", "repo/branch/my-file.txt")
    sha = tx.commit("repo", "branch", message="Add my-file.txt")

# after transaction completion, just use the SHA value as normal.
fs.get_file(f"repo/{sha.id}/my-file.txt", "my-new-file.txt")
```

## Thread safety

Through its use of `collections.deque` as a store for uploads, upload queueing and file transfers are thread-safe.
