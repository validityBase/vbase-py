#!/bin/bash
# Run the tests against the localhost test chain in config/.env.localhost.
# Pass test names to run only those; by default all tests run.
# Set PYTHON_BIN if the environment's interpreter is not python3, as in a
# Windows virtual environment, where it is python.

set -a
source config/.env.localhost
set +a

PYTHON_BIN="${PYTHON_BIN:-python3}"

# The localhost image deploys the commitment contract after its node starts.
"${PYTHON_BIN}" vbase/tests/scripts/wait_for_localhost_contract.py || exit 1

if [[ $# -eq 0 ]]; then
    set -- discover -s vbase/tests
fi
"${PYTHON_BIN}" -m unittest "$@"
