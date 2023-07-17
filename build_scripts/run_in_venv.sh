#!/usr/bin/env bash

# This script is used to invoke a command in a guaranteed virtualenv.
# If a virtualenv is already active, the command is executed directly.
# Otherwise, `poetry run` is used to execute the command.
#
# Compared to using `poetry run` all the time, it provides faster
# execution times when the venv is already active.

IS_WINDOWS=${WSL_DISTRO_NAME:+1:-0}

if [[ -z "$VIRTUAL_ENV" ]]; then
    # Not in an active Poetry venv -- find Poetry and run the desired command through it

    POETRY_EXECUTABLE=poetry${IS_WINDOWS:+.exe}
    POETRY=$(which "$POETRY_EXECUTABLE")

    if [[ ! -x "$POETRY" ]]; then
        echo "poetry not found or not executable, trying fallback location"

        # VS Code sets up $PATH in a strange way, so Poetry might not be found sometimes
        POETRY=$HOME/.poetry/bin/poetry
        if [[ ! -x "$POETRY" ]]; then
            echo "poetry fallback '$POETRY' not found, exiting."
            exit 1
        fi
    fi

    exec $POETRY run "$@"
else
    # Inside a Poetry venv -- run the command directly
    "$@"
fi
