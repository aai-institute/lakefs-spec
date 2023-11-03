#!/bin/bash -ex

INFILE=pyproject.toml
COMMON_OPTIONS=(--no-annotate)

# Treat positional arguments as packages to be upgraded
while [[ $# -gt 0 ]]; do
    COMMON_OPTIONS+=(-P "$1")
    shift
done

# Lock (and, if specified, upgrade) packages
pip-compile "${COMMON_OPTIONS[@]}" --strip-extras "$INFILE"
pip-compile "${COMMON_OPTIONS[@]}" --extra=dev --output-file=requirements-dev.txt "$INFILE"
pip-compile "${COMMON_OPTIONS[@]}" --extra=docs --output-file=requirements-docs.txt "$INFILE"
