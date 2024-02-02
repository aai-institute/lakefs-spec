# Using transactions on the lakeFS file system

In addition to file operations, you can carry out versioning operations in your Python code using file system *transactions*.

Transactions in lakeFS-spec behave similarly to the transactions in the [high-level lakeFS SDK](https://docs.lakefs.io/integrations/python.html#transactions):
Both approaches create an ephemeral branch for a transaction, perform the operations in the context block on that ephemeral branch, and optionally merge it back into the source branch upon exiting the context manager.

They are an "all or nothing" proposition: If an error occurs during the transaction, the base branch is left unchanged.

The lakeFS-spec transaction inherits from fsspec transactions. For more information on fsspec transactions, see the [official documentation](https://filesystem-spec.readthedocs.io/en/latest/features.html#transactions).

## Versioning operations

The lakeFS file system's transaction is the intended place for conducting versioning operations between file transfers.
The following is an example of file uploads with commit creations, with a tag being applied at the end.

```python
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

with fs.transaction("repo", "main") as tx:
    fs.put_file("train-data.txt", f"repo/{tx.branch.id}/train-data.txt")
    tx.commit(message="Add training data")
    fs.put_file("test-data.txt", f"repo/{tx.branch.id}/test-data.txt")
    sha = tx.commit(message="Add test data")
    tx.tag(sha, name="My train-test split")
```

The full list of supported lakeFS versioning operations (by default, these operations target the transaction branch):

* [`commit`](../reference/lakefs_spec/transaction.md#lakefs_spec.transaction.LakeFSTransaction.commit), for creating a commit, optionally with attached metadata.
* [`merge`](../reference/lakefs_spec/transaction.md#lakefs_spec.transaction.LakeFSTransaction.merge), for merging a given branch.
* [`revert`](../reference/lakefs_spec/transaction.md#lakefs_spec.transaction.LakeFSTransaction.revert), for reverting a previous commit.
* [`rev_parse`](../reference/lakefs_spec/transaction.md#lakefs_spec.transaction.LakeFSTransaction.rev_parse), for parsing revisions like branch/tag names and SHA fragments into full commit SHAs.
* [`tag`](../reference/lakefs_spec/transaction.md#lakefs_spec.transaction.LakeFSTransaction.tag), for creating a tag pointing to a commit.

## Lifecycle of ephemeral transaction branches

You can control the lifecycle for a transaction branch with the `delete` argument:

* By default (`delete="onsuccess`), the branch is deleted after successful completion, and left over in case of failure for debugging purposes.
* If `delete="always"`, the branch is unconditionally deleted after the transaction regardless of its status.
* Similarly, if `delete="never"`, the branch is unconditionally left in place after the transaction.

Additionally, the `automerge` keyword controls whether the transaction branch is merged after successful completion of the transaction. 
It has no effect if an error occurs over the course of the transaction.

## Error handling

Since all files are uploaded to a short-lived transaction branch, no commit on the target branch happens in case of an exception:

```python
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

with fs.transaction("repo", "main", delete="onsuccess") as tx:
    fs.put_file("my-file.txt", f"repo/{tx.branch.id}/my-file.txt")
    tx.commit(message="Add my-file.txt")
    raise ValueError("oops!")
```

The above code will not modify the `main` branch, since the `ValueError` prevents the merge of the transaction branch.
Note that you can examine the contents of the transaction branch due to `delete="onsuccess"` (the default behavior), which prevents deletion of the branch in case of failure for debugging purposes.
