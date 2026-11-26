#!/bin/bash

set -a
source config/.env.localhost
set +a

# The test chain deploys the commitment contract after the node starts.
# Wait for the contract so that the tests do not start before it exists.
python3 - <<'EOF' || exit 1
import os
import sys
import time

from web3 import Web3

w3 = Web3(Web3.HTTPProvider(os.environ["VBASE_COMMITMENT_SERVICE_NODE_RPC_URL"]))
address = os.environ["VBASE_COMMITMENT_SERVICE_ADDRESS"]
for _ in range(60):
    try:
        if w3.eth.get_code(address):
            sys.exit(0)
    except Exception:  # The node may not accept connections yet.
        pass
    time.sleep(1)
sys.exit(f"No commitment contract at {address} after 60 seconds.")
EOF

python3 -m unittest discover -s vbase/tests
