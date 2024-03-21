"""Automatically generate API reference pages from source files.

Source: https://mkdocstrings.github.io/recipes/#automatic-code-reference-pages

Note: this script assumes a source layout with a `src/` folder.
"""

import ast
import logging
from pathlib import Path

import docstring_parser
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

    mkdocs_gen_files.set_edit_path(full_doc_path, path)

# Add links for top-level modules to root page
root_page = next(it for it in nav.items() if it.level == 0)
children = [it for it in nav.items() if it.level == 1]

with mkdocs_gen_files.open(f"reference/{root_page.filename}", "a") as f:
    f.write("## Modules\n")
    for ch in children:
        f.write(f"### [{ch.title}](../{ch.filename})\n")

        try:
            source_file = Path("src", ch.filename).with_suffix(".py")

            # Index page for submodules maps to __init__.py of the module
            if source_file.stem == "index":
                source_file = source_file.with_stem("__init__")

            tree = ast.parse(source_file.read_text())
            docstring = ast.get_docstring(tree, clean=False)
            doc = docstring_parser.parse(docstring)

            if doc.short_description:
                f.write(f"{doc.short_description}\n\n")
        except Exception as e:
            logging.warning(f"Could not parse module docstring: {ch.filename}", exc_info=True)

with mkdocs_gen_files.open("reference/SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())
