#!/bin/bash

set -euo pipefail

set -a
source config/.env.forwarder.pub.dev
set +a

# Isolate each public dev run from the address history and nonce state left by
# earlier runs. The Forwarder pays for the transaction, so this signer does not
# need funds and can be discarded after the tests finish.
VBASE_COMMITMENT_SERVICE_PRIVATE_KEY="$(
    python3 -c 'from eth_account import Account; key = Account.create().key.hex(); print(key if key.startswith("0x") else f"0x{key}")'
)"
export VBASE_COMMITMENT_SERVICE_PRIVATE_KEY

python3 -m unittest vbase.tests.test_vbase_client
python3 -m unittest vbase.tests.test_indexing_service
