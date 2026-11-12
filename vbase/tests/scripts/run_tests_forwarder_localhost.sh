#!/usr/bin/env bash
# Run the forwarder tests against a forwarder service on this machine.
# Set VBASE_FORWARDER_PORT if the forwarder does not listen on port 8081.
# Pass test names to run only those; by default all forwarder modules run.
# See "Localhost Forwarder Tests" in internal/specs/development.md.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../../.." && pwd)"
cd "${REPO_ROOT}"

set -a
source config/.env.forwarder.localhost
set +a

: "${VBASE_API_KEY:?VBASE_API_KEY must hold an API key that the localhost forwarder accepts}"

PYTHON_BIN="${PYTHON_BIN:-python3}"

# The forwarder pays gas, so the signer needs no funds. A fresh address for
# each run keeps the tests' commitments and cleared state separate from other
# runs and from the direct localhost tests.
VBASE_COMMITMENT_SERVICE_PRIVATE_KEY="$("${PYTHON_BIN}" -c 'from eth_account import Account; print("0x" + bytes(Account.create().key).hex())')"
export VBASE_COMMITMENT_SERVICE_PRIVATE_KEY

"${PYTHON_BIN}" vbase/tests/scripts/check_forwarder.py

if [[ $# -eq 0 ]]; then
    set -- \
        vbase.tests.test_vbase_client \
        vbase.tests.test_indexing_service \
        vbase.tests.test_vbase_dataset \
        vbase.tests.test_vbase_dataset_async \
        vbase.tests.test_vbase_dataset_bootstrap \
        vbase.tests.test_sim
fi

# One unittest process runs all modules, so the exit status covers every test.
"${PYTHON_BIN}" -m unittest "$@"
