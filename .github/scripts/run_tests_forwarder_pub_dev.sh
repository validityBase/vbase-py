#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"

set -a
source "${REPO_ROOT}/config/.env.forwarder.pub.dev"
set +a

: "${VBASE_API_KEY:?VBASE_API_KEY must be supplied by the environment or a secret manager}"

if [[ -z "${PYTHON_BIN:-}" ]]; then
    if command -v python3 >/dev/null 2>&1; then
        PYTHON_BIN=python3
    elif command -v python >/dev/null 2>&1; then
        PYTHON_BIN=python
    else
        echo "Neither python3 nor python was found." >&2
        exit 1
    fi
fi

# Use a fresh signer for each invocation so old event history and nonce state
# cannot affect later runs. Both test modules share this signer.
VBASE_COMMITMENT_SERVICE_PRIVATE_KEY="$("${PYTHON_BIN}" -c 'from eth_account import Account; print("0x" + bytes(Account.create().key).hex())')"
export VBASE_COMMITMENT_SERVICE_PRIVATE_KEY

"${PYTHON_BIN}" "${REPO_ROOT}/.github/scripts/run_tests_forwarder_pub_dev.py" "$@"
