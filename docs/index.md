# lakefs-spec: An fsspec backend for lakeFS

!!! tip "Early Adopters"
    We are seeking early adopters who would like to actively participate in our feedback process and shape the future of the library.
    If you are interested in using the library and want to get in touch with us, please reach out via [Github Discussions](https://github.com/aai-institute/lakefs-spec/discussions){: target="_blank" rel="noopener"}.

Welcome to lakeFS-spec, a [filesystem-spec](https://github.com/fsspec/filesystem_spec){: target="_blank" rel="noopener"} backend implementation for the [lakeFS](https://lakefs.io/){: target="_blank" rel="noopener"} data lake.
Our primary goal is to streamline versioned data operations in lakeFS, enabling seamless integration with popular data science tools such as Pandas, Polars, and DuckDB directly from Python.

Highlights:

- High-level abstraction over basic lakeFS repository operations
- Seamless integration into the fsspec ecosystem
- Transaction support
- Zero-config option through config autodiscovery
- Automatic up-/download management to avoid unnecessary transfers for unchanged files

<hr>

::cards:: cols=3

- title: Quickstart
  content: Step-by-step installation and first operations
  icon: ":octicons-flame-24:{ .landing-page-icon }"
  url: quickstart.md

- title: Tutorials
  content: In-depth tutorials on using lakefs-spec
  icon: ":octicons-repo-clone-24:{ .landing-page-icon }"
  url: tutorials/index.md

- title: API Reference
  content: Full documentation of the Python API
  icon: ":octicons-file-code-24:{ .landing-page-icon }"
  url: reference/lakefs_spec/index.md

- title: User Guide
  content: Solving specific tasks with lakefs-spec
  icon: ":octicons-tasklist-24:{ .landing-page-icon }"
  url: guides/index.md

- title: Contributing
  content: How to contribute to the project
  icon: ":octicons-code-of-conduct-24:{ .landing-page-icon }"
  url: CONTRIBUTING.md

::/cards::
