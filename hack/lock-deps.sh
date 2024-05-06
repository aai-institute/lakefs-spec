#!/bin/bash -ex

INFILE=pyproject.toml
COMMON_OPTIONS=(--no-annotate --no-strip-extras)

# Treat positional arguments as packages to be upgraded
while [[ $# -gt 0 ]]; do
    COMMON_OPTIONS+=(-P "$1")
    shift
done

# Lock (and, if specified, upgrade) packages
uv pip compile "${COMMON_OPTIONS[@]}" --extra=dev --output-file=requirements-dev.txt "$INFILE"
uv pip compile "${COMMON_OPTIONS[@]}" --extra=docs --output-file=requirements-docs.txt "$INFILE"
