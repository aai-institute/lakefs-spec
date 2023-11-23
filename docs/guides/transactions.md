# Using transactions on the lakeFS file system

A transaction defers file transfers and versioning operations to a queue, which is unwound sequentially on completion.
Transactions are thread-safe and atomic, meaning that a single failure during any transaction function causes the entire transaction to abort.

TODO: Add content
