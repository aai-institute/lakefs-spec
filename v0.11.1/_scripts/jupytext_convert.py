"""Automatically convert Jupytext files to Notebooks"""

import logging
from pathlib import Path

import jupytext
import mkdocs_gen_files

for path in sorted(Path("docs/tutorials").rglob("*.py")):
    ipynb_path = path.relative_to(Path("docs")).with_suffix(".ipynb")
    logging.debug(f"Converting {path} -> {ipynb_path}")

    jupytext_file = path.read_text(encoding="utf-8")
    jupytext_nb = jupytext.reads(jupytext_file, fmt="py:percent")

    with mkdocs_gen_files.open(ipynb_path, "wb") as fd:
        content = jupytext.writes(jupytext_nb, fmt="notebook")
        fd.write(content.encode("utf8"))

    mkdocs_gen_files.set_edit_path(ipynb_path, path)
