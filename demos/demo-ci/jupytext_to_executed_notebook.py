import datetime
import os
import sys
from pathlib import Path

import jupytext
import nbformat
from loguru import logger
from nbconvert.preprocessors import ExecutePreprocessor


def jupytext_to_executed_notebook(
    jupytext_path: os.PathLike, executed_notebook_path: os.PathLike
) -> None:
    """Reads a [jupytext](https://jupytext.readthedocs.io/en/latest/) file, executes it,
    and stores the notebook with results as `.ipynb` file.

    Parameters
    ----------
    jupytext_path : os.PathLike
        Path to a jupytext File
    executed_notebook_path : os.PathLike
        Path where the generated notebook is supposed to be stored, stored as `.ipynb`.
    """
    jupytext_path = Path(jupytext_path)
    executed_notebook_path = Path(executed_notebook_path)

    jupytext_code = jupytext.read(fp=str(jupytext_path), fmt="py:percent")
    ExecutePreprocessor().preprocess(jupytext_code)

    executed_notebook_path.parent.mkdir(exist_ok=True, parents=True)

    nbformat.write(
        nb=jupytext_code, fp=str(executed_notebook_path.with_suffix(".ipynb"))
    )


def main() -> None:
    """Reads `sys.argv` for a path to a file or directory containing `.py` files with
    [jupytext](https://jupytext.readthedocs.io/en/latest/), executes them, and stores
    the results as `.ipynb` in the provided output file or directory.

    Usage: python jupytext_to_executed_notebook.py in_file_or_dir out_file_or_dir
    """
    if len(sys.argv) < 3:
        logger.error("Not enough arguments provided")
        logger.info(
            "Usage: python jupytext_to_executed_notebook.py in_file_or_dir out_file_or_dir"
        )
        sys.exit(1)

    in_path = Path(sys.argv[1])
    out_path = Path(sys.argv[2])

    in_files = []
    if in_path.is_dir():
        in_files = list(sorted(in_path.glob("**/*.py")))
    else:
        in_files = [in_path]

    for f in in_files:
        if in_path.is_dir():
            relative_in_file = f.relative_to(in_path)
            out_file = out_path / relative_in_file.with_suffix(".ipynb")
            out_file.parent.mkdir(parents=True, exist_ok=True)
        else:
            out_file = out_path.with_suffix(".ipynb")
            out_file.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"Executing Jupytext {f}, storing executed notebook in {out_file}")

        start_time = datetime.datetime.now()
        jupytext_to_executed_notebook(f, out_file)
        end_time = datetime.datetime.now()

        logger.debug(f"Executing {f} took {end_time - start_time}")


if __name__ == "__main__":
    main()
