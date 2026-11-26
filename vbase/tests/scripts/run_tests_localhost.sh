#!/bin/bash
# Run the tests against the localhost test chain in config/.env.localhost.
# Pass test names to run only those; by default all tests run.

set -a
source config/.env.localhost
set +a

# The localhost image deploys the commitment contract after its node starts.
python3 vbase/tests/scripts/wait_for_localhost_contract.py || exit 1

if [[ $# -eq 0 ]]; then
    set -- discover -s vbase/tests
fi
python3 -m unittest "$@"
