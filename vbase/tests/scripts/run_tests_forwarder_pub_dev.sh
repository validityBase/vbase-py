#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)"

set -a
source "${REPO_ROOT}/config/.env.forwarder.pub.dev"
set +a

: "${VBASE_API_KEY:?VBASE_API_KEY must be supplied by the environment or a secret manager}"

# Each invocation gets a fresh address, avoiding shared event history and nonce
# state. Both test modules use the same signer for this invocation.
VBASE_COMMITMENT_SERVICE_PRIVATE_KEY="$(python3 -c 'from eth_account import Account; print("0x" + bytes(Account.create().key).hex())')"
export VBASE_COMMITMENT_SERVICE_PRIVATE_KEY

cd "${REPO_ROOT}"
if [[ "$#" -eq 0 ]]; then
    set -- vbase.tests.test_vbase_client vbase.tests.test_indexing_service
fi
python3 -m unittest "$@"
