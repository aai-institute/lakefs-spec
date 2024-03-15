"""Automatically generate API reference pages from source files.

Source: https://mkdocstrings.github.io/recipes/#automatic-code-reference-pages
"""

from pathlib import Path

import mkdocs_gen_files

nav = mkdocs_gen_files.Nav()

for path in sorted(Path("src").rglob("*.py")):
    module_path = path.relative_to("src").with_suffix("")
    doc_path = path.relative_to("src").with_suffix(".md")
    full_doc_path = Path("reference", doc_path)

    parts = list(module_path.parts)

    if parts[-1] == "__init__":
        parts = parts[:-1]
        doc_path = doc_path.with_name("index.md")
        full_doc_path = full_doc_path.with_name("index.md")
    elif parts[-1] == "__main__":
        continue

    nav[parts] = doc_path.as_posix()

    with mkdocs_gen_files.open(full_doc_path, "w") as fd:
        identifier = ".".join(parts)
        print("::: " + identifier, file=fd)

        if identifier == "lakefs_spec":
            print("\nSee also:\n", file=fd)
            print(
                "- [errors](errors.md): Module for error handling and custom exceptions.",
                file=fd,
            )
            print(
                "- [spec](spec.md): Main module defining the lakeFS filesystem specification.",
                file=fd,
            )
            print(
                "- [transaction](transaction.md): Module for handling transactions in the lakeFS filesystem.",
                file=fd,
            )
            print(
                "- [util](util.md): Utility functions and helper classes for the lakeFS filesystem.",
                file=fd,
            )
    mkdocs_gen_files.set_edit_path(full_doc_path, path)

with mkdocs_gen_files.open("reference/SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())
